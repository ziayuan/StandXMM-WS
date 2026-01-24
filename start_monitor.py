import asyncio
import time
import os
import logging
from dotenv import load_dotenv
from protocol.ws_client import StandXPerpWS
from protocol.auth import StandXAuth # Ensure this uses the latest fixed version
from utils.notifier import Notifier
from utils.config_loader import load_config

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(name)s] %(levelname)s: %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger("Monitor")

class PositionMonitor:
    def __init__(self):
        load_dotenv()
        self.config = load_config()
        
        # Init components
        self.auth = StandXPerpWS().auth # Reuse the logic from ws_client init which loads from env
        
        # We need a dedicated WS client for monitoring
        self.ws = StandXPerpWS()
        self.notifier = Notifier(self.config)
        
        # State
        self.positions = {} # {symbol: start_time}
        self.last_alert_time = 0
        
    async def start(self):
        logger.info("🛡️ Starting Position Monitor...")
        
        # Define Callback
        async def on_position(data):
            # Format: {"channel":"position", "data": {"symbol":"BTC-USD", "qty":"0.1", ...}}
            # Note: Verify actual payload structure from logs
            p = data.get("data", {})
            symbol = p.get("symbol")
            qty = float(p.get("qty", 0))
            
            if qty != 0:
                # Position Open
                if symbol not in self.positions:
                    logger.info(f"🚨 Position Opened: {symbol} Size: {qty}")
                    self.positions[symbol] = time.time()
                else:
                    # Update? Keep original start time
                    pass
            else:
                # Position Closed
                if symbol in self.positions:
                    duration = time.time() - self.positions[symbol]
                    logger.info(f"✅ Position Closed: {symbol} (Held {duration:.1f}s)")
                    del self.positions[symbol]

        # Subscribe
        self.ws.subscribe("position", callback=on_position)
        
        # Start WS in background
        asyncio.create_task(self.ws.start())
        
        # Start Monitoring Loop
        await self.monitor_loop()

    async def monitor_loop(self):
        logger.info("👀 Monitoring loop active. Threshold: 30s")
        while True:
            await asyncio.sleep(1)
            now = time.time()
            
            # Check Connections
            if self.ws.ws_market is None:
                 # Warn if disconnected?
                 pass

            # Check Positions
            for symbol, start_time in list(self.positions.items()):
                duration = now - start_time
                if duration > 30:
                    await self.trigger_alert(symbol, duration)

    async def trigger_alert(self, symbol: str, duration: float):
        # Debounce alerts (every 10s)
        if time.time() - self.last_alert_time < 10:
            return
            
        msg = f"⚠️ DANGER: {symbol} held for {int(duration)}s! Network Error?"
        logger.warning(msg)
        
        # 1. Telegram
        await self.notifier.alert(msg)
        
        # 2. Local Sound (Mac)
        # Using non-blocking way if possible, or short blocking
        try:
            os.system(f'say "Danger. {symbol} held too long." &')
        except:
            print('\a') # Beep fallback
            
        self.last_alert_time = time.time()

if __name__ == "__main__":
    try:
        mon = PositionMonitor()
        asyncio.run(mon.start())
    except KeyboardInterrupt:
        pass
    except Exception as e:
        print(f"Fatal Error: {e}")
