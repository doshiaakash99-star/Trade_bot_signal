import yfinance as yf
import pandas as pd
import logging
import requests
import yaml
from datetime import datetime, timedelta
import pytz
import schedule
import time
import os
import tempfile
from pathlib import Path
from google.cloud import storage

# Setup logging with file output
log_dir = Path('logs')
log_dir.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_dir / 'bot.log'),
        logging.StreamHandler()
    ]
)

# Constants
BASE_DIR = Path(__file__).resolve().parent
CONFIG_FILE = BASE_DIR / 'trade_bot_secrets.yml'
SYMBOL = '^NSEI'
CSV_FILE = 'data/nifty_data_2003.csv'
IST = pytz.timezone('Asia/Kolkata')
FAST_EMA_PERIOD = 7
SLOW_EMA_PERIOD = 14
TREND_SMA_PERIOD = 100
MAX_RETRIES = 3
RETRY_DELAY = 5
MARKET_OPEN_TIME = datetime.strptime('09:15', '%H:%M').time()
MARKET_CLOSE_TIME = datetime.strptime('15:30', '%H:%M').time()
GCS_BUCKET = os.getenv('GCS_BUCKET', '').strip()
GCS_OBJECT = os.getenv('GCS_OBJECT', CSV_FILE).strip()
BOT_RUN_MODE = os.getenv('BOT_RUN_MODE', 'single').strip().lower()


def load_sensitive_config(config_file=CONFIG_FILE):
    """
    Load sensitive values from environment variables or YAML config file.
    Environment variables take precedence over file values.
    """
    config = {}

    if config_file.exists():
        try:
            with open(config_file, 'r', encoding='utf-8') as file:
                loaded = yaml.safe_load(file) or {}
                if isinstance(loaded, dict):
                    config = loaded
                else:
                    logging.warning(f"Invalid config format in {config_file}, expected key-value mapping")
        except Exception as exc:
            logging.error(f"Error loading config file {config_file}: {exc}")

    telegram_bot_token = os.getenv('TELEGRAM_BOT_TOKEN', config.get('TELEGRAM_BOT_TOKEN', '')).strip()
    chat_id = os.getenv('CHAT_ID', str(config.get('CHAT_ID', ''))).strip()

    if not telegram_bot_token or not chat_id:
        raise ValueError(
            f"Missing TELEGRAM_BOT_TOKEN or CHAT_ID. Set env vars or update {config_file.resolve()}."
        )

    return telegram_bot_token, chat_id


TELEGRAM_BOT_TOKEN, CHAT_ID = load_sensitive_config()

# Ensure data directory exists
Path('data').mkdir(exist_ok=True)

# Global variable to track last signal and market state
last_signal = None
last_signal_time = None


def get_runtime_csv_file():
    """
    Resolve the CSV path for the current runtime.
    Cloud Run uses a temporary local file and syncs it with GCS.
    """
    if GCS_BUCKET:
        temp_dir = Path(tempfile.gettempdir()) / 'shoonya-data'
        temp_dir.mkdir(parents=True, exist_ok=True)
        return str(temp_dir / Path(GCS_OBJECT).name)

    csv_path = Path(CSV_FILE)
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    return str(csv_path)


def download_csv_from_gcs(local_csv_file):
    """
    Download the historical CSV from Google Cloud Storage if configured.
    """
    if not GCS_BUCKET:
        return

    try:
        client = storage.Client()
        blob = client.bucket(GCS_BUCKET).blob(GCS_OBJECT)

        if blob.exists():
            Path(local_csv_file).parent.mkdir(parents=True, exist_ok=True)
            blob.download_to_filename(local_csv_file)
            logging.info(f"Downloaded historical CSV from gs://{GCS_BUCKET}/{GCS_OBJECT}")
        else:
            logging.info(f"Historical CSV not found at gs://{GCS_BUCKET}/{GCS_OBJECT}; a new file will be created")
    except Exception as exc:
        logging.error(f"Error downloading CSV from GCS: {exc}")
        raise


def upload_csv_to_gcs(local_csv_file):
    """
    Upload the historical CSV to Google Cloud Storage if configured.
    """
    if not GCS_BUCKET or not os.path.exists(local_csv_file):
        return

    try:
        client = storage.Client()
        blob = client.bucket(GCS_BUCKET).blob(GCS_OBJECT)
        blob.upload_from_filename(local_csv_file)
        logging.info(f"Uploaded historical CSV to gs://{GCS_BUCKET}/{GCS_OBJECT}")
    except Exception as exc:
        logging.error(f"Error uploading CSV to GCS: {exc}")
        raise

def is_market_open(current_time=None):
    """
    Check if market is open during trading hours.
    Market hours: 09:15 to 15:30 IST (Mon-Fri)
    """
    if current_time is None:
        current_time = datetime.now(IST)
    
    # Check if weekday (0=Monday, 4=Friday, 5-6=Weekend)
    if current_time.weekday() >= 5:  # Saturday or Sunday
        return False
    
    current_time_only = current_time.time()
    return MARKET_OPEN_TIME <= current_time_only <= MARKET_CLOSE_TIME

def send_market_closed_alert():
    """
    Send alert when market closes.
    """
    try:
        current_time = datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S')
        message = f"MARKET CLOSED\n\nTime: {current_time}\nPlease check back tomorrow during market hours (09:15 - 15:30 IST)\n\nBot will resume trading signals during next market session."
        
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {'chat_id': CHAT_ID, 'text': message}
        logging.debug(f"Sending market closed alert to Telegram Chat ID: {CHAT_ID}")
        response = requests.post(url, data=payload, timeout=10)
        
        if response.status_code == 200:
            logging.info("Market closed alert sent successfully to Telegram")
            return True
        else:
            logging.error(f"Failed to send market closed alert. Status: {response.status_code}, Response: {response.text}")
            return False
    except Exception as e:
        logging.error(f"Error sending market closed alert: {str(e)}")
        return False

def fetch_data(start, end, interval='1h', retries=MAX_RETRIES):
    """
    Fetch data with retry logic and better error handling.
    """
    for attempt in range(retries):
        try:
            data = yf.download(SYMBOL, start=start, end=end, interval=interval, progress=False)

            if data.empty:
                logging.warning("No data fetched from yfinance")
                return pd.DataFrame()

            # Keep only required columns
            data = data[['Open', 'High', 'Low', 'Close', 'Volume']]

            # Flatten MultiIndex columns if present
            data.columns = [col[0] if isinstance(col, tuple) else col for col in data.columns]

            # Handle MultiIndex in rows
            if isinstance(data.index, pd.MultiIndex) and 'Ticker' in data.index.names:
                data = data.droplevel('Ticker')

            # Ensure timezone-aware
            if data.index.tz is None:
                data.index = data.index.tz_localize(pytz.UTC).tz_convert(IST)
            else:
                data.index = data.index.tz_convert(IST)

            logging.info(f"Fetched {len(data)} rows of data from yfinance")
            return data

        except Exception as e:
            logging.error(f"Error fetching data (attempt {attempt + 1}/{retries}): {e}")
            if attempt < retries - 1:
                time.sleep(RETRY_DELAY)
            else:
                return pd.DataFrame()


def update_csv(df, csv_file):
    """
    Update CSV file with new data, avoiding duplicates and column mismatch.
    """
    if df.empty:
        return

    try:
        # Flatten columns if MultiIndex
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        required_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
        df = df[required_cols].copy()

        # Ensure timezone consistency
        if df.index.tz is None:
            df.index = df.index.tz_localize(IST)
        else:
            df.index = df.index.tz_convert(IST)

        # Load existing data
        if os.path.exists(csv_file):
            existing = pd.read_csv(csv_file, index_col=0, parse_dates=True)

            if isinstance(existing.index, pd.DatetimeIndex):
                if existing.index.tz is None:
                    existing.index = existing.index.tz_localize(IST)
                else:
                    existing.index = existing.index.tz_convert(IST)

            existing = existing[required_cols]
            existing = existing.apply(pd.to_numeric, errors='coerce').dropna()

            # Combine safely
            combined = pd.concat([existing, df])
            combined = combined[~combined.index.duplicated(keep='last')]
            combined.sort_index(inplace=True)
        else:
            combined = df.copy()

        # Save with error handling
        combined.to_csv(csv_file)
        logging.info(f"Updated {csv_file} with {len(combined)} rows")

    except Exception as e:
        logging.error(f"Error updating CSV: {e}")


def calculate_indicators(df):
    """
    Calculate EMA7, EMA14, SMA100 indicators with validation.
    """
    if len(df) < TREND_SMA_PERIOD:
        logging.warning(f"Not enough data for SMA{TREND_SMA_PERIOD} calculation ({len(df)} rows)")
        return pd.DataFrame()

    try:
        df = df.copy()
        df['ema_fast'] = df['Close'].ewm(span=FAST_EMA_PERIOD).mean()
        df['ema_slow'] = df['Close'].ewm(span=SLOW_EMA_PERIOD).mean()
        df['sma_trend'] = df['Close'].rolling(window=TREND_SMA_PERIOD).mean()
        df.dropna(inplace=True)
        return df
    except Exception as e:
        logging.error(f"Error calculating indicators: {e}")
        return pd.DataFrame()


def generate_signals(df):
    """
    Generate trading signals based on EMA crossover and SMA filter.
    """
    if len(df) < 2:
        return None

    try:
        latest = df.iloc[-1]
        prev = df.iloc[-2]

        # Long signal
        if (prev['ema_fast'] <= prev['ema_slow'] and latest['ema_fast'] > latest['ema_slow'] and
            latest['ema_fast'] > latest['sma_trend'] and latest['ema_slow'] > latest['sma_trend']):
            return 'BUY'
        # Short signal
        elif (prev['ema_fast'] >= prev['ema_slow'] and latest['ema_fast'] < latest['ema_slow'] and
              latest['ema_fast'] < latest['sma_trend'] and latest['ema_slow'] < latest['sma_trend']):
            return 'SELL'
        # Exit signal
        elif ((prev['ema_fast'] > prev['ema_slow'] and latest['ema_fast'] < latest['ema_slow']) or
              (prev['ema_fast'] < prev['ema_slow'] and latest['ema_fast'] > latest['ema_slow'])):
            return 'EXIT'
        return None
    except Exception as e:
        logging.error(f"Error generating signals: {e}")
        return None


def send_telegram_alert(message, retries=MAX_RETRIES):
    """
    Send alert via Telegram bot with retry logic.
    """
    for attempt in range(retries):
        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
            payload = {'chat_id': CHAT_ID, 'text': message}
            response = requests.post(url, data=payload, timeout=10)
            
            if response.status_code == 200:
                logging.info("Telegram alert sent successfully")
                return True
            else:
                logging.error(f"Failed to send Telegram alert: {response.text}")
        except Exception as e:
            logging.error(f"Error sending Telegram alert (attempt {attempt + 1}/{retries}): {e}")
            if attempt < retries - 1:
                time.sleep(RETRY_DELAY)
    return False


def check_and_send_signal(df):
    """
    Check for new signals and send alert if different from last signal.
    """
    global last_signal, last_signal_time
    signal = generate_signals(df)
    
    if signal and signal != last_signal:
        try:
            price = df.iloc[-1]['Close']
            timestamp = df.index[-1].strftime('%Y-%m-%d %H:%M:%S')
            message = f"NIFTY SIGNAL: {signal}\nTime: {timestamp}\nPrice: {price:.2f}"
            send_telegram_alert(message)
            last_signal = signal
            last_signal_time = datetime.now(IST)
            logging.info(f"New signal generated: {signal} at {price:.2f}")
        except Exception as e:
            logging.error(f"Error in check_and_send_signal: {e}")


def job(csv_file=CSV_FILE, current_time=None):
    """
    Main job: fetch data, calculate signals, and send alerts.
    Only runs during market hours (09:15 - 15:30 IST).
    """
    try:
        now = current_time or datetime.now(IST)
        current_time = now.strftime('%Y-%m-%d %H:%M:%S')
        
        # Skip if market is closed
        if not is_market_open(now):
            logging.debug(f"Market is CLOSED at {current_time}, skipping job")
            return

        # Fetch latest data
        end = now
        start = now - timedelta(days=60) if not os.path.exists(csv_file) else now - timedelta(days=2)
        df = fetch_data(start, end)
        
        if df.empty:
            logging.warning("Failed to fetch data in job")
            return
        
        update_csv(df, csv_file)

        # Load and process data
        df = pd.read_csv(csv_file, index_col=0, parse_dates=True)
        numeric_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
        df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')
        df = df.dropna(subset=numeric_cols)
        
        df = calculate_indicators(df)
        if df.empty:
            logging.warning("No valid indicators calculated")
            return
        
        check_and_send_signal(df)

    except Exception as e:
        logging.error(f"Error in job execution: {e}")


def initial_setup(csv_file=CSV_FILE, send_status_message=True):
    """
    Initial setup: fetch historical data and save to CSV if not exists.
    """
    logging.info("Running initial setup")
    try:
        # Test Telegram credentials
        if send_status_message:
            logging.info(f"Testing Telegram connection with Chat ID: {CHAT_ID}")
            test_message = f"Bot started at {datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S IST')}"
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

            try:
                response = requests.post(url, data={'chat_id': CHAT_ID, 'text': test_message}, timeout=10)
                if response.status_code == 200:
                    logging.info("Telegram credentials verified successfully")
                else:
                    logging.warning(f"Telegram test failed: {response.status_code} - {response.text}")
            except Exception as telegram_test_error:
                logging.warning(f"Could not verify Telegram connection: {telegram_test_error}")
        
        now = datetime.now(IST)
        
        if not os.path.exists(csv_file):
            logging.info("CSV file not found, fetching 60 days of historical data")
            start = now - timedelta(days=60)
            df = fetch_data(start, now)
            if df.empty:
                logging.error("Failed to fetch initial data")
                return
            update_csv(df, csv_file)
        else:
            logging.info("CSV file exists, updating with recent data")
            start = now - timedelta(days=2)
            df = fetch_data(start, now)
            if not df.empty:
                update_csv(df, csv_file)

        # Validate data
        if os.path.exists(csv_file):
            df = pd.read_csv(csv_file, index_col=0, parse_dates=True)
            numeric_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
            df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')
            df = df.dropna(subset=numeric_cols)
            logging.info(f"Initial setup completed with {len(df)} data points")

    except Exception as e:
        logging.error(f"Error during initial setup: {e}")


def run_single_cycle():
    """
    Execute one trading cycle for Cloud Run / hourly scheduler usage.
    """
    runtime_csv_file = get_runtime_csv_file()

    try:
        download_csv_from_gcs(runtime_csv_file)

        current_time = datetime.now(IST)
        if not is_market_open(current_time):
            logging.info(
                f"Market is CLOSED at {current_time.strftime('%Y-%m-%d %H:%M:%S IST')}. Single run will exit."
            )
            return 0

        job(csv_file=runtime_csv_file, current_time=current_time)
        upload_csv_to_gcs(runtime_csv_file)
        logging.info("Single trading cycle completed successfully")
        return 0
    except Exception as exc:
        logging.error(f"Single cycle execution failed: {exc}")
        return 1


def run_continuous_bot():
    """
    Run the original continuous scheduler loop for local execution.
    """
    current_time = datetime.now(IST)

    if not is_market_open(current_time):
        raise ValueError(
            f"Market is CLOSED. Current time: {current_time.strftime('%Y-%m-%d %H:%M:%S IST')}\n"
            f"Market hours: 09:15 - 15:30 IST (Mon-Fri). Please start bot during market hours."
        )

    logging.info(f"Market is OPEN. Bot started at {current_time.strftime('%Y-%m-%d %H:%M:%S IST')}")
    logging.info("Bot will exit automatically at 15:30 IST when market closes.")

    try:
        startup_message = f"Bot Started\n\nTime: {current_time.strftime('%Y-%m-%d %H:%M:%S IST')}\nMarket Status: OPEN\nBot will monitor signals every hour and close at 15:30 IST"
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        response = requests.post(url, data={'chat_id': CHAT_ID, 'text': startup_message}, timeout=10)
        if response.status_code == 200:
            logging.info("Startup notification sent to Telegram")
        else:
            logging.warning(f"Could not send startup notification: {response.text}")
    except Exception as startup_error:
        logging.warning(f"Could not send startup notification: {startup_error}")

    initial_setup(send_status_message=True)
    schedule.every().hour.at(":16").do(job)
    logging.info("Scheduler configured to run jobs every hour at :16 minutes.")

    while True:
        now = datetime.now(IST)

        if now.time() > MARKET_CLOSE_TIME:
            logging.info(f"Market closed at {now.strftime('%Y-%m-%d %H:%M:%S IST')}. Bot will now exit.")
            send_market_closed_alert()
            break

        schedule.run_pending()
        time.sleep(60)

    logging.info("Bot exited gracefully after market close.")


if __name__ == '__main__':
    try:
        if BOT_RUN_MODE == 'continuous':
            run_continuous_bot()
        else:
            raise SystemExit(run_single_cycle())
    except KeyboardInterrupt:
        logging.info("Bot stopped by user (Ctrl+C)")
        if is_market_open():
            logging.info("Sending market close alert...")
            send_market_closed_alert()
    except ValueError as setup_error:
        error_msg = str(setup_error)
        logging.error(f"Setup Error: {error_msg}")
        try:
            # Send market closed notification to Telegram
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
            payload = {'chat_id': CHAT_ID, 'text': error_msg}
            response = requests.post(url, data=payload, timeout=10)
            if response.status_code == 200:
                logging.info("Market closed notification sent to Telegram")
            else:
                logging.warning(f"Could not send Telegram notification: {response.text}")
        except Exception as tg_error:
            logging.warning(f"Could not send Telegram notification: {tg_error}")
    except Exception as e:
        logging.error(f"Fatal error: {e}")