import asyncio
import json
import time
import logging
import websockets
import os
from typing import Dict, Any, Callable, Optional, List
from .auth import StandXAuth

logger = logging.getLogger("StandXWS")

class StandXPerpWS:
    """
    Robust WebSocket Client for StandX.
    Handles auto-reconnection, heartbeats, and auth.
    """
    def __init__(self):
        # API Key Auth
        self.api_key = os.getenv("STANDX_API_KEY")
        self.api_secret = os.getenv("STANDX_API_SECRET")
        self.chain = os.getenv("STANDX_CHAIN", "arbitrum") 
        
        if not self.api_key or not self.api_secret:
             # Fallback for user transition or error
             raise ValueError("Missing STANDX_API_KEY or STANDX_API_SECRET in .env")
             
        self.auth = StandXAuth(self.api_key, self.api_secret, self.chain)
        
        self.ws_url = "wss://perps.standx.com/ws-stream/v1"
        self.ws_api_url = "wss://perps.standx.com/ws-api/v1"
        
        # State
        self.running = False
        self.ws_market = None
        self.ws_trading = None
        
        # Callbacks: channel -> function
        self.callbacks: Dict[str, Callable] = {}
        self.callbacks: Dict[str, Callable] = {}
        self.subscriptions: List[dict] = []
        self.balance: Dict[str, Any] = {} # Store balance info
        
        # Watchdog
        self.last_data_time = time.time()      # Time of last legitimate WS frame (for System Alert)
        self.last_activity_time = time.time()  # Time of last activity (for Internal Reconnect)
        
    async def start(self):
        """Start the WebSocket manager."""
        self.running = True
        self.last_data_time = time.time()
        self.last_activity_time = time.time()
        await asyncio.gather(
             self._maintain_connection("market", self.ws_url),
             self._maintain_connection("trading", self.ws_api_url),
             self._watchdog()
        )

    async def _watchdog(self):
        """Monitor connection health."""
        logger.info("[Watchdog] Started")
        while self.running:
            await asyncio.sleep(5)
            # If no activity for 30s, assume dead and reconnect
            if time.time() - self.last_activity_time > 30:
                logger.warning(f"[Watchdog] No activity for {int(time.time() - self.last_activity_time)}s. Forcing reconnect...")
                # Close sockets to trigger maintain_connection loop exception/break
                if self.ws_market: await self.ws_market.close()
                if self.ws_trading: await self.ws_trading.close()
                # Reset activity timer to prevent immediate loop, but NOT data timer
                self.last_activity_time = time.time() 

    def subscribe(self, channel: str, symbol: Optional[str] = None, callback: Callable = None):
        sub_msg = {"subscribe": {"channel": channel}}
        if symbol: sub_msg["subscribe"]["symbol"] = symbol
        self.subscriptions.append(sub_msg)
        key = f"{channel}:{symbol}" if symbol else channel
        if callback: self.callbacks[key] = callback
            
    async def place_order(self, order_params: dict) -> dict:
        if not self.ws_trading: raise ConnectionError("Trading WS not connected")
            
        request_id = f"ord-{int(time.time()*1000)}"
        
        # Params must be stringified JSON according to docs
        # Use compact separators to ensure signature matches (no spaces)
        payload_str = json.dumps(order_params, separators=(',', ':'))
        
        # Get Signature Headers
        timestamp = int(time.time() * 1000)
        # Call the new sign_request
        headers = self.auth.sign_request(payload_str, request_id, timestamp)
        
        msg = {
            "method": "order:new",
            "params": payload_str,
            "request_id": request_id,
            "session_id": self.auth.get_public_id(), # Critical: Must match signing key
            "header": headers 
        }
        
        await self.ws_trading.send(json.dumps(msg))
        return msg

    async def cancel_orders(self, order_ids: List[str], symbol: str, is_client_id: bool = False):
        if not self.ws_trading: raise ConnectionError("Trading WS not connected")
        
        for oid in order_ids:
             params = {"symbol": symbol}
             if is_client_id:
                 params["client_order_id"] = oid
                 params["cl_ord_id"] = oid 
             else:
                 params["order_id"] = oid
                 
             payload_str = json.dumps(params, separators=(',', ':'))
             request_id = f"can-{oid}"
             timestamp = int(time.time() * 1000)
             headers = self.auth.sign_request(payload_str, request_id, timestamp)
             
             msg = {
                "method": "order:cancel",
                "params": payload_str,
                "request_id": request_id,
                "session_id": self.auth.get_public_id(),
                "header": headers
             }
             
             await self.ws_trading.send(json.dumps(msg))

    async def _maintain_connection(self, name: str, url: str):
        retry_delay = 1
        while True:
            try:
                # Standard connection (No URL Token)
                async with websockets.connect(url, ping_interval=None) as ws:
                    logger.info(f"[{name}] Connected to {url}")
                    if name == "market":
                        self.ws_market = ws
                        await self._on_connect_market(ws)
                    else:
                        self.ws_trading = ws
                        await self._on_connect_trading(ws)
                        
                    retry_delay = 1
                    async for message in ws:
                        await self._handle_message(name, message)
            except Exception as e:
                logger.warning(f"[{name}] Reconnecting ({e})...")
            
            if name == "market": self.ws_market = None
            else: self.ws_trading = None
            await asyncio.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, 60)
            
    async def send_command(self, method: str, params: dict):
        """Send a command (RPC) to the Trading Stream."""
        if not self.ws_trading: return
        
        timestamp = int(time.time() * 1000)
        request_id = f"cmd-{method}-{timestamp}"
        params_json = json.dumps(params, separators=(',', ':'))
        
        # Sign the PARAMS (which is the payload)
        sig_headers = self.auth.sign_request(params_json, request_id, timestamp)
        
        msg = {
            "session_id": self.auth.get_public_id(), # Use Derived Public Key
            "request_id": request_id,
            "method": method,
            "header": sig_headers,
            "params": params_json
        }
        await self.ws_trading.send(json.dumps(msg))
        return request_id

    async def fetch_balance(self):
        """Fetch account balance via Trading WS."""
        if not self.ws_trading: return None
        # Send command and hope for response in handle_message (or just send and let user see log?)
        # For a true "fetch", we need to wait for response. 
        # But our architecture is async callback based.
        # For now, we will send the command. The response will be logged in `[Trading] Msg: ...`
        # To make it usable in Status Report, we need to store the balance in self.balance state.
        return await self.send_command("account:state", {})

    async def _on_connect_market(self, ws):
        # Market Stream Auth (Token Login)
        try:
            token = self.auth.get_jwt() # Returns API Key
            
            streams = []
            for sub in self.subscriptions:
                if "subscribe" in sub:
                     streams.append(sub["subscribe"])
                else:
                     streams.append(sub)
            
            # Use Standard Token Login
            login_msg = {
                "auth": {
                    "token": token,
                    "streams": streams
                }
            }
            
            logger.info("[Market] Sending Token Login + Subs...")
            await ws.send(json.dumps(login_msg))
            
            # Robustness: Send explicit subscribe requests for all channels
            # This ensures even if 'streams' auth param missed something, we get it.
            await asyncio.sleep(0.5) 
            for sub in self.subscriptions:
                await ws.send(json.dumps(sub))
            
        except Exception as e:
            logger.error(f"[Market] Login failed: {e}")
            
    async def _on_connect_trading(self, ws):
        # Trading Stream Auth (Token Login)
        try:
            token = self.auth.get_jwt() # Returns API Key
            
            # Use Standard RPC Login
            # Note: Params must be a string for some endpoints, but here 'auth:login' usually takes object?
            # Docs said: { "token": "<jwt>" }
            # Wait, docs example: "params": "{\"token\":\"...\"}" (Stringified JSON)
            
            login_params = {"token": token}
            
            login_msg = {
                "method": "auth:login",
                "params": json.dumps(login_params), # Stringified!
                "request_id": "auth-init"
            }

            logger.info("[Trading] Sending Token Login...")
            await ws.send(json.dumps(login_msg))
            
        except Exception as e:
            logger.error(f"[Trading] Login failed: {e}")

    async def _handle_message(self, name: str, message: str):
        self.last_data_time = time.time()
        self.last_activity_time = time.time()
        try:
            if message == "ping": return

            data = json.loads(message)
            channel = data.get("channel")
            
            # Auth Response Handling
            if channel == "auth":
                response = data.get("data", {})
                if response.get("code") == 0 or response.get("msg") == "success":
                    logger.info("[Market] Auth Success")
                else:
                    logger.error(f"[Market] Auth Fail: {response}")
                return

            # Dispatch
            if channel:
                symbol = data.get("symbol")
                key = f"{channel}:{symbol}" if symbol else channel
                if key in self.callbacks: await self.callbacks[key](data)
                elif channel in self.callbacks: await self.callbacks[channel](data)
            
            # Concise Logging for User Data
            if name == "market":
                 if "code" in data and data["code"] != 0:
                      logger.info(f"[Market] Err: {data}")
                 
                 if channel == "order":
                     d = data.get("data", {})
                     # Log: Order [ID] [Side] [Qty] [Status]
                     logger.info(f"[Order] {d.get('side')} {d.get('order_type')} {d.get('qty')} @ {d.get('price')} | Status: {d.get('status')} | ID: {d.get('cl_ord_id')}")
                 
                 elif channel == "position":
                     d = data.get("data", {})
                     # Log: Position [Symbol] [Size] [Entry]
                     logger.info(f"[Position] {d.get('symbol')} Size: {d.get('qty')} @ {d.get('entry_price')} | PnL: {d.get('realized_pnl')}")

                 elif channel == "balance":
                     # Log Balance updates to debug
                     d = data.get("data", {})
                     logger.info(f"[Balance] Total: {d.get('total')} | Free: {d.get('free')}")

            # Debug: Log Trading Responses
            if name == "trading":
                # Check for Account State Response (Balance)
                # Structure guess: {"data": {"balances": [...]}, "request_id": "..."}
                # Or {"result": ...}
                # We will log it first.
                logger.info(f"[Trading] Msg: {data}")
                
                # Try to parse balance if it looks like account info
                # Assuming response to account:state or channel account
                # This is heuristic until we confirm structure
                if "balances" in str(data) or "available_balance" in str(data):
                     res = data.get("data", {}) or data.get("result", {})
                     if res:
                         self.balance = res
                         logger.info(f"[Balance] Updated: {self.balance}")

        except Exception as e:
            logger.error(f"[{name}] Parse error: {e}")
