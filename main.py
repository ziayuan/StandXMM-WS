import asyncio
import yaml
import logging
import os
import signal
import time
from dotenv import load_dotenv
from protocol.auth import StandXAuth
from alert.notifier import Notifier
from protocol.ws_client import StandXPerpWS
from strategy.market_maker import StandXMarketMaker

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(name)s] %(levelname)s: %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger("Main")

# Load Env using absolute path to be sure
env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
load_dotenv(env_path)
logger.info(f"Loaded .env from {env_path}")

# Security: Ensure Exchange Traffic Bypasses Proxy (Ladder)
# This ensures that even if local proxy fails, we can still close orders.
os.environ["no_proxy"] = "*" # Allow all direct, or specific "standx.com"
# Actually, for safety, let's bypass standx.com explicitly and maybe others.
# If user has global proxy, we might want to bypass everything to be safe?
# User said "Local Network" can access exchange.
# no_proxy format: comma separated domains.
os.environ["no_proxy"] = "standx.com,api.standx.com,perps.standx.com"
logger.info(f"Configured Direct Network (no_proxy): {os.environ['no_proxy']}")

def load_config(path="config.yaml"):
    if not os.path.exists(path):
        raise FileNotFoundError(f"Config not found at {path}")
    with open(path, 'r') as f:
        return yaml.safe_load(f)

async def shutdown(loop, ws_client, strategy):
    logger.info("Shutdown signal received")
    
    # 1. Cancel Open Orders
    if strategy:
        try:
            await asyncio.wait_for(strategy.cancel_all(), timeout=5.0)
        except Exception as e:
            logger.error(f"Error cancelling orders: {e}")
            
    # 2. Stop WS Client (if needed) works by stopping loop mostly
    
    # 3. Cancel Tasks
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    [task.cancel() for task in tasks]
    await asyncio.gather(*tasks, return_exceptions=True)
    loop.stop()

async def system_watchdog(ws_client, notifier, timeout=60):
    """Monitor global activity and alert if frozen."""
    logger.info(f"System Watchdog Started (Timeout: {timeout}s)")
    while True:
        await asyncio.sleep(10)
        
        # Check last message time
        last_active = ws_client.last_data_time
        silence_duration = time.time() - last_active
        
        if silence_duration > timeout:
            msg = f"No data received for {int(silence_duration)}s! Bot may be frozen."
            logger.warning(msg)
            await notifier.alert(msg)
            # Debounce: Wait a bit before alerting again to avoid spam
            await asyncio.sleep(30) 

async def async_main():
    config = load_config()
    
    # ... (Key Loading) ...
    # Init Notifier
    notifier = Notifier(config)
    
    # Initialize WS Client (Auto-loads API Key from Env)
    try:
        ws_client = StandXPerpWS()
    except Exception as e:
        logger.error(f"Failed to init WS Client: {e}")
        return
    
    strategy = StandXMarketMaker(
        ws_client=ws_client,
        symbol=config['symbol'],
        config=config
    )
    
    # Signals
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(shutdown(loop, ws_client, strategy)))

    # Init Strategy (Subscriptions)
    await strategy.start()
    
    # Start System Watchdog
    wd_cfg = config.get("system_watchdog", {})
    wd_timeout = wd_cfg.get("timeout_seconds", 60)
    asyncio.create_task(system_watchdog(ws_client, notifier, wd_timeout))
    
    # Start Telegram Command Listener
    async def msg_status():
        report = strategy.get_status_report()
        await notifier.alert(report) # reuse alert to send msg

    async def msg_stop():
        await notifier.alert("🛑 Stopping Bot via Command...")
        await strategy.stop_trading()
        # Trigger graceful shutdown
        loop = asyncio.get_running_loop()
        # We can simulate SIGTERM or just cancel things.
        # Calling shutdown directly:
        await shutdown(loop, ws_client, strategy)

    async def emergency_stop():
        logger.critical("🚨 Proxy Failure Detected! Initiating Emergency Stop locally.")
        try:
            # 1. Sound Alert (Local)
            logger.info("Playing Sound Alert...")
            await notifier.play_sound()
        except Exception as e:
            logger.error(f"Sound failed: {e}")

        try:
            # 2. Stop Strategy (Local)
            await strategy.stop_trading()
        except Exception as e:
            logger.error(f"Stop Trading failed: {e}")

        try:
            # 3. Shutdown (Local)
            await shutdown(loop, ws_client, strategy)
        except Exception as e:
            logger.error(f"Shutdown failed: {e}")
            
        logger.critical("Emergency Stop Complete. Forcing Exit.")
        os._exit(1)
        
    asyncio.create_task(notifier.poll_commands(msg_status, msg_stop, emergency_stop))
    
    # Start WS Connection Loop
    await ws_client.start()


if __name__ == "__main__":
    try:
        asyncio.run(async_main())
    except KeyboardInterrupt:
        # Handled by signal handler usually, or loop stops
        pass
