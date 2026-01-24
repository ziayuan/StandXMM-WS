import os
import logging
import asyncio
import aiohttp
import platform
import subprocess
import time

logger = logging.getLogger("Notifier")

class Notifier:
    def __init__(self, config: dict):
        self.config = config.get("system_watchdog", {})
        self.enabled_sound = self.config.get("sound_alert", False)
        self.enabled_telegram = self.config.get("telegram_alert", False)
        
        self.tg_token = os.getenv("TELEGRAM_BOT_TOKEN")
        self.tg_chat_id = os.getenv("TELEGRAM_CHAT_ID")
        
        if self.enabled_telegram and (not self.tg_token or not self.tg_chat_id):
            logger.warning("Telegram Alert enabled but Token/ChatID missing in .env")
            self.enabled_telegram = False
            
        # Proxy for Telegram (Separate from System Proxy)
        self.proxy_url = config.get("telegram", {}).get("proxy_url")

    async def alert(self, message: str):
        """Trigger all enabled alerts."""
        logger.warning(f"🚨 ALERT: {message}")
        
        tasks = []
        if self.enabled_sound:
            tasks.append(self.play_sound())
        if self.enabled_telegram:
            tasks.append(self.send_telegram(message))
            
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def play_sound(self):
        """Play system sound (Mac/Linux)."""
        try:
            system = platform.system()
            if system == "Darwin": # Mac
                # Play a system sound (e.g., Sosumi or Ping)
                process = await asyncio.create_subprocess_exec(
                    "afplay", "/System/Library/Sounds/Ping.aiff",
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL
                )
                await process.wait()
            else:
                # Fallback for Linux usually has 'aplay' or generic beep
                print("\a") # Bell char
        except Exception as e:
            logger.error(f"Sound alert failed: {e}")

    async def send_telegram(self, message: str):
        """Send message to Telegram."""
        if not self.tg_token or not self.tg_chat_id: return
        
        url = f"https://api.telegram.org/bot{self.tg_token}/sendMessage"
        payload = {
            "chat_id": self.tg_chat_id,
            "text": f"🚨 [StandX Bot] Alert:\n{message}"
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                # Use explicit proxy if configured
                async with session.post(url, json=payload, timeout=5, proxy=self.proxy_url) as resp:
                    if resp.status != 200:
                        err = await resp.text()
                        logger.error(f"Telegram send failed: {err}")
                    else:
                        logger.info("Telegram alert sent.")
        except Exception as e:
            logger.error(f"Telegram network error: {e}")

    async def poll_commands(self, on_status_request, on_stop_request, on_network_fail=None):
        """Poll Telegram for /status and /stop commands."""
        if not self.enabled_telegram:
            logger.warning("Telegram Polling Disabled (Missing Config)")
            return
        
        logger.info(f"Telegram Command Listener Started (Proxy: {self.proxy_url or 'System'})")
        
        # Capture startup time to ignore old commands (like the one that stopped us previously)
        boot_time = time.time()
        
        offset = 0
        url = f"https://api.telegram.org/bot{self.tg_token}/getUpdates"
        
        consecutive_fails = 0
        MAX_FAILURES = 3 
        
        async with aiohttp.ClientSession() as session:
            while True:
                try:

                    # Long Poll: Wait 10s for updates on Server side
                    payload = {"offset": offset, "timeout": 10}
                    
                    # Client Timeout: Wait 20s total (giving 10s buffer for network lag)
                    # If this times out, it means Network/Proxy is truly dead.
                    async with session.get(url, params=payload, timeout=20, proxy=self.proxy_url) as resp:
                        if resp.status != 200:
                            logger.error(f"TG Poll Error: {resp.status}")
                            consecutive_fails += 1
                            if consecutive_fails >= MAX_FAILURES: 
                                logger.critical("Too many Telegram errors! Proxy might be dead.")
                                if on_network_fail: await on_network_fail()
                                
                            await asyncio.sleep(3) # Faster retry
                            continue
                        
                        # Success
                        consecutive_fails = 0 # Reset counter on any success
                            
                        data = await resp.json()
                        result = data.get("result", [])
                        
                        for update in result:
                            offset = update["update_id"] + 1
                            message = update.get("message", {})
                            text = message.get("text", "").strip()
                            chat_id = str(message.get("chat", {}).get("id"))
                            
                            # Debug: Log incoming
                            logger.info(f"TG Update: '{text}' from {chat_id} (Expected: {self.tg_chat_id})")
                            
                            # Security Check: Only allow My Chat
                            if chat_id != str(self.tg_chat_id):
                                logger.warning(f"Ignored command from unknown chat: {chat_id}")
                                continue
                                
                            # Stale Check: Ignore commands Sent BEFORE this run started
                            msg_date = message.get("date", 0)
                            if msg_date < boot_time:
                                logger.warning(f"Ignored PRE-BOOT command '{text}' (Age: {boot_time - msg_date:.1f}s)")
                                continue
                                
                            if text == "/status":
                                logger.info("Received /status command")
                                await on_status_request()
                            elif text == "/stop":
                                logger.info("Received /stop command")
                                await on_stop_request()
                                
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    # Specific error message for proxy issues
                    logger.error(f"TG Poll Exception: {e}")
                    consecutive_fails += 1
                    if consecutive_fails >= MAX_FAILURES:
                         logger.critical("Too many Network Errors! Proxy might be dead.")
                         if on_network_fail: await on_network_fail()
                         
                    await asyncio.sleep(5)
