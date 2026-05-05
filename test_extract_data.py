import yfinance as yf
from datetime import datetime, timedelta

# Define the symbol and today's date
symbol = "^NSEI"
today = datetime.now()
period = '3d'

# Check if today is a trading day and fetch data
try:
    print(f"Attempting to fetch intraday data for {symbol} with period={period}...")
    data = yf.Ticker(symbol).history(period=period, interval='1h', auto_adjust=False, actions=False)

    if data.empty:
        print(f"No intraday data available for {symbol}. Please check the symbol or market status.")
    else:
        print(f"Intraday data fetched successfully for {symbol}:")
        print(data)

except Exception as e:
    print(f"An error occurred while fetching data: {e}")