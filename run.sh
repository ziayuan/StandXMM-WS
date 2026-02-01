#!/bin/bash

# Ensure we are in the script directory
cd "$(dirname "$0")" || exit 1

echo "🚀 Starting StandX Bot in BACKGROUND mode..."
echo "Logs will be written to: bot.log"

# Install deps if needed (optional)
# ./venv/bin/pip install -r requirements.txt

# Run with nohup (No Hang Up)
# -u: Unbuffered output (forces logs to write immediately)
# > bot.log 2>&1: Redirect both stdout and stderr to bot.log
# &: Run in background
nohup ./venv/bin/python3 -u main.py > bot.log 2>&1 &

PID=$!
echo "✅ Bot started! PID: $PID"
echo "---------------------------------------------------"
echo "📄 To view logs:  tail -f bot.log"
echo "🛑 To stop bot:   kill $PID"
echo "---------------------------------------------------"
