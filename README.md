# StandX Market Maker Bot

A high-performance, WebSocket-based Market Making bot for the **StandX Perpetual Exchange**.
Built with Python `asyncio` for low-latency order execution and real-time state management.

## 🚀 Features

- **Grid Strategy**: Automatically places buy/sell orders around the market price to capture spread.
- **WebSocket Streaming**: Uses StandX's real-time `Market` and `Trading` WebSocket streams.
- **Robust Authentication**: Supports Ed25519 Signing with secure internal handling of API Keys (Base58/Base64 support).
- **Latency Monitoring**: Includes tools to test connectivity to StandX servers from your deployment env.
- **Safety Watchdogs**:
    - **System Watchdog**: Detects if the data feed freezes and alerts you.
    - **Position Monitor**: (`start_monitor.py`) Independent process to alert (Sound + Telegram) if a position is held too long (e.g. > 30s), indicating network or logic hang.
- **Telegram Integration**: Sends alerts and errors directly to your Telegram.

## 📂 Project Structure

- `main.py`: Entry point for the trading bot.
- `strategy/`: Contains the Grid Trading logic (`StandXMarketMaker`).
- `protocol/`: Handles WebSocket connections (`ws_client.py`) and Authentication (`auth.py`).
- `check_latency.py`: Tool to test network speed to StandX.
- `start_monitor.py`: Independent safety monitor.
- `debug_key.py`: Utility to verify your API Key format.

## 🛠️ Installation

1.  **Clone the repository**:
    ```bash
    git clone https://github.com/ziayuan/StandXMM-WS.git
    cd StandXMM-WS
    ```

2.  **Set up Python Environment**:
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt
    ```

3.  **Configure Secrets**:
    Copy the example config:
    ```bash
    cp .env.example .env
    ```
    Edit `.env` and fill in your StandX API Key/Secret and Telegram details.

4.  **Adjust Strategy**:
    Edit `config.yaml` to set your target `symbol`, `quantity`, and grid parameters (`spread`, `step`).

## 🏃‍♂️ Usage

### 1. Run the Bot
```bash
./run.sh
# OR manually:
source venv/bin/activate
python main.py
```

### 2. Run Network Test (Recommended for Servers)
Check if your server has a stable connection to StandX.
```bash
python check_latency.py
```

### 3. Run Safety Monitor (Optional but Recommended)
Run this in a separate terminal or process. It acts as a "Dead Man's Switch" alarm.
```bash
python start_monitor.py
```

## ⚠️ Disclaimer

This software is for educational purposes only. Market making involves significant risk.
**Use at your own risk.** The authors are not responsible for any financial losses incurred.
