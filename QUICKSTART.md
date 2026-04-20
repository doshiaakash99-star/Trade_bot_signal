# NIFTY Trading Bot - Quick Start Guide

## Setup Instructions (One-Time)

### 1. Create Desktop Shortcut
Double-click the `create_shortcut.vbs` file in the bot directory. This will create a shortcut on your Desktop.

```
create_shortcut.vbs  [Double-click this]
```

You should see a message: "Shortcut created successfully!"

A shortcut named **"NIFTY Trading Bot"** will appear on your Desktop.

---

## Daily Usage

### Option 1: Using Desktop Shortcut (Recommended)
1. **Double-click** the "NIFTY Trading Bot" shortcut on your Desktop
2. A command window will open showing:
   - Bot status
   - Market status
   - Start time
3. Bot will run from market open (09:15 IST) until 15:30 IST
4. Window will close automatically when bot exits

### Option 2: Manual Execution
1. Open command prompt or PowerShell
2. Navigate to bot directory:
   ```
   cd C:\Users\Aakash_Doshi\Desktop\Shoonya\ShoonyaApi-py-master\ShoonyaApi-py-master
   ```
3. Run:
   ```
   .\.venv\Scripts\python.exe Trade_signal_bot_updated_vs.py
   ```

---

## What Happens When Bot Starts (During Market Hours)

✅ **Telegram Notifications:**
- Bot sends startup message: `"Bot Started - Time: XX:XX:XX IST\nMarket Status: OPEN\nBot will monitor signals until 15:30 IST"`
- Every hour at :16 minutes → Trading signals (BUY/SELL/EXIT)
- At 15:30 IST → Market closed message + Bot exits

✅ **Console Logging:**
- All events logged to: `logs/bot.log`
- Real-time output in command window

---

## Error Cases

### Case 1: Bot Started Outside Market Hours
**Output in Telegram:**
```
Market is CLOSED. Current time: 17:30:00 IST
Market hours: 09:15 - 15:30 IST (Mon-Fri). Please start bot during market hours.
```

**Action:** Wait until 09:15 IST next trading day and restart.

### Case 2: Virtual Environment Not Found
**Error in window:**
```
ERROR: Virtual environment not found!
Please ensure .venv is properly set up.
```

**Fix:** Run these commands in PowerShell:
```powershell
cd C:\Users\Aakash_Doshi\Desktop\Shoonya\ShoonyaApi-py-master\ShoonyaApi-py-master
python -m venv .venv
.\.venv\Scripts\pip install -r requirements.txt
```

### Case 3: Telegram Connection Failed
- Bot will still run locally
- Check log file: `logs/bot.log`
- Verify Telegram credentials in: `trade_bot_secrets.yml`

---

## Files You Need

| File | Purpose |
|------|---------|
| `START_BOT.bat` | Batch script to start bot (created) |
| `create_shortcut.vbs` | Creates Desktop shortcut (created) |
| `Trade_signal_bot_updated_vs.py` | Main bot script |
| `.venv/` | Python virtual environment |
| `trade_bot_secrets.yml` | Telegram credentials |
| `logs/bot.log` | Bot execution logs |

---

## Troubleshooting

### Bot won't start?
1. Check if market is open (09:15-15:30 IST Mon-Fri)
2. Verify Telegram token and Chat ID in `trade_bot_secrets.yml`
3. Check logs: `logs/bot.log`
4. Ensure you have internet connection

### Not getting Telegram notifications?
1. Verify credentials: `trade_bot_secrets.yml`
   ```yaml
   TELEGRAM_BOT_TOKEN: "your_actual_token_here"
   CHAT_ID: "your_actual_chat_id_here"
   ```
2. Check Telegram app is updated
3. Ensure firewall allows outbound HTTPS to api.telegram.org

### Bot closes unexpectedly?
1. Check `logs/bot.log` for errors
2. If error mentions market close time, wait until 15:30 IST passed
3. Restart bot during market hours

---

## Telegram Message Format

**Startup Message:**
```
Bot Started

Time: 2026-03-20 09:30:00 IST
Market Status: OPEN
Bot will monitor signals every hour and close at 15:30 IST
```

**Signal Message (Example):**
```
NIFTY SIGNAL: BUY
Time: 2026-03-20 10:30:00
Price: 23450.50
```

**Market Close Message:**
```
MARKET CLOSED

Time: 2026-03-20 15:30:00
Please check back tomorrow during market hours (09:15 - 15:30 IST)

Bot will resume trading signals during next market session.
```

---

**Questions?** Check the logs in `logs/bot.log` for detailed execution information.
