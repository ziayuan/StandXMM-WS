import asyncio
import logging
import time
import math
from typing import List, Dict

logger = logging.getLogger("StandXStrategy")

class StandXMarketMaker:
    """
    Event-Driven Market Maker for StandX.
    """
    def __init__(self, ws_client, symbol: str, config: dict):
        self.ws = ws_client
        self.symbol = symbol
        self.config = config
        
        # Grid Params
        self.price_spread = config['grid']['price_spread']
        self.price_step = config['grid']['price_step']
        self.grid_count = config['grid']['grid_count']
        self.order_qty = str(config['grid']['order_quantity'])
        self.fill_cooldown_minutes = config['grid'].get('fill_cooldown_minutes', 10)
        
        # State
        self.last_price = 0.0
        self.position_size = 0.0
        # pending_orders: Price -> {id: str, side: str, qty: str}
        self.pending_orders: Dict[float, dict] = {} 
        self.my_orders_snapshot = [] # List of open orders from WS
        self.is_running = True
        self.resume_time = 0 # Timestamp to resume trading (Cool Down)
        self.balance_info = {} # Store balance from WS subscription
        
        # Locks
        self.lock = asyncio.Lock()
        
    def get_status_report(self) -> str:
        """Return a formatted status string."""
        lines = [
            f"📊 **StandX Bot Status**",
            f"Symbol: `{self.symbol}`",
            f"Price: `{self.last_price}`",
            f"Position: `{self.position_size}`"
        ]
        
        # Add Balance Info from WS subscription
        if self.balance_info:
            total = self.balance_info.get("total", "?")
            free = self.balance_info.get("free", "?")
            token = self.balance_info.get("token", "")
            lines.append(f"💰 Balance: `{total}` {token} (Free: `{free}`)")
        else:
            lines.append(f"💰 Balance: (Waiting...)")
             
        lines.extend([
            f"Running: `{self.is_running}`",
            f"**Pending Orders ({len(self.pending_orders)}):**"
        ])
        
        # Sort by price descending
        sorted_orders = sorted(self.pending_orders.items(), key=lambda x: x[0], reverse=True)
        for price, info in sorted_orders:
            side = info.get('side', '?').upper()
            qty = info.get('qty', '?')
            lines.append(f"- {side} {qty} @ {price}")
            
        return "\n".join(lines)
        
    async def stop_trading(self):
        """Stop placing new orders and cancel existing."""
        self.is_running = False
        await self.cancel_all()
        # Note: Auto-close logic in on_position is still active if WS is running,
        # which is good for 'clearing inventory'.
        
    async def start(self):
        """Subscribe to channels and register callbacks."""
        logger.info(f"Starting Strategy for {self.symbol}")
        
        # Subscribe to Price Channel (Official Mid-Price & Spread)
        # This is much more robust than managing raw depth deltas locally.
        self.ws.subscribe("price", self.symbol, self.on_price_update)
        
        # Subscribe to User Orders (to track fills/cancels)
        self.ws.subscribe("order", None, self.on_order_update)
        
        # Subscribe to Position (for Auto-Close / Risk)
        self.ws.subscribe("position", None, self.on_position)
        
        # Subscribe to Balance (for /status command)
        self.ws.subscribe("balance", None, self.on_balance)

    async def on_balance(self, data: dict):
        """Handle balance updates from WS."""
        try:
            bal = data.get("data", {})
            if bal:
                self.balance_info = bal
        except Exception as e:
            logger.error(f"Balance error: {e}")

    async def on_price_update(self, data: dict):
        """
        Called when price updates.
        Data format:
        {
            "channel": "price",
            "data": {
                "mid_price": "121898.00",
                "spread": ["121897.95", "121898.05"],
                ...
            }
        }
        """
        try:
            d = data.get("data", {})
            mid_price_str = d.get("mid_price")
            
            if not mid_price_str:
                return

            mid_price = float(mid_price_str)
            
            # Update State
            self.last_price = mid_price
            
            # Use lock to prevent race conditions
            if not self.lock.locked():
                await self.rebalance_grid()

        except Exception as e:
            logger.error(f"Price processing error: {e}")


        
    async def on_position(self, data: dict):
        """
        Handle position updates (Push or Poll Response).
        """
        try:
            # Check format. Push: {channel: position, data: [...]}. Poll Resp: {result: [...]}
            positions = []
            if "data" in data:
                positions = data["data"]
            elif "result" in data:
                positions = data["result"]
            
            # Ensure it's a list
            if isinstance(positions, dict): positions = [positions]
            
            for pos in positions:
                if pos.get("symbol") == self.symbol:
                    # Fix: StandX uses 'qty' for position size in WS updates
                    size = float(pos.get("qty", 0) or pos.get("size", 0))
                    self.position_size = size # Track size for Status Report
                    
                    if size != 0:
                        logger.warning(f"🔴 Position: {size} | Auto-Closing...")
                        await self._close_position_market(size)
        except Exception as e:
            logger.error(f"Position error: {e} | Data: {str(data)[:100]}")

    async def _close_position_market(self, size: float):
        side = "sell" if size > 0 else "buy"
        qty = str(abs(size))
        try:
            params = {
                "symbol": self.symbol,
                "side": side,
                "order_type": "market",
                "qty": qty,
                "reduce_only": True
            }
            # Cl Ord ID for tracking
            params["cl_ord_id"] = f"close-{int(time.time()*1000)}"
            await self.ws.place_order(params)
        except Exception as e:
            logger.error(f"Close failed: {e}")

    async def on_order_update(self, data: dict):
        """
        Called when my order status changes.
        """
        # data: {"channel": "order", "data": {...} OR [...]}
        try:
            raw_data = data.get("data")
            if isinstance(raw_data, list):
                orders = raw_data
            elif isinstance(raw_data, dict):
                orders = [raw_data]
            else:
                return

            for o in orders:
                status = str(o.get("status")).lower() # filled, canceled, etc
                cl_ord_id = o.get("client_order_id") or o.get("cl_ord_id")
                
                # Logic 1: Auto-Close REMOVED. 
                # We relay purely on on_position to handle total position closure.
                # This prevents partial closing when multiple fills occur.

                # Logic 2: Clear Pending Map so we can Re-fill
                if status in ["filled", "canceled", "expired", "rejected"]:
                    found_price = None
                    async with self.lock:
                        for price, info in self.pending_orders.items():
                            if info.get("id") == cl_ord_id:
                                found_price = price
                                break
                        
                        if found_price is not None:
                            del self.pending_orders[found_price]

                # Logic 3: Pause Logic (Cool Down)
                if status == "filled":
                     pause_sec = self.fill_cooldown_minutes * 60
                     logger.warning(f"🟢 FILL! Pausing {self.fill_cooldown_minutes}min...")
                     self.resume_time = time.time() + pause_sec
                     await self.cancel_all()
                            
        except Exception as e:
            logger.error(f"Order Update error: {e}")
            
    async def rebalance_grid(self):
        """
        Core Logic:
        1. Calculate desired Grid Prices (Long/Short).
        2. Get current open orders.
        3. Diff -> Cancel unneeded, Place missing.
        """
        if not self.is_running: return # Stop trading flag
        if self.last_price <= 0: return

        # Cool Down Check
        if time.time() < self.resume_time:
            return

        async with self.lock:
            # 1. Generate Target Grid
            target_longs, target_shorts = self._generate_grid_prices(self.last_price)
            
            # 2. Get Current Active orders (from local tracking or simple assumption)
            # In a robust system, we track `on_order_update`. 
            # For V1, we might simply cancel-all-and-replace (aggressive & waste quota) 
            # OR we need to track what we have placed.
            # Let's try to be smart: Track `self.pending_orders` map.
            
            # Identifying stale orders
            to_cancel = []
            valid_prices = set(target_longs + target_shorts)
            
            # Check existing orders
            # But wait, self.pending_orders is Local. What if we restarted?
            # Ideally we fetch open orders via REST on startup (TODO).
            # For now assume clean slate.
            
            current_prices = list(self.pending_orders.keys())
            
            for price in current_prices:
                if price not in valid_prices:
                    # Order at this price is no longer needed
                    info = self.pending_orders[price]
                    to_cancel.append(info["id"])
                    # Optimistically remove from map
                    del self.pending_orders[price]
            
            # Identifying missing orders
            to_place = []
            for price in target_longs:
                if price not in self.pending_orders:
                    to_place.append({"side": "buy", "price": price})
                    
            for price in target_shorts:
                if price not in self.pending_orders:
                    to_place.append({"side": "sell", "price": price})
            
            # Execute Actions (Silent - only log fills/errors)
            
            # 3. Batch Cancel
            if to_cancel:
                await self.ws.cancel_orders(to_cancel, self.symbol, is_client_id=True)
            
            # 4. Batch/Seq Place
            for order in to_place:
                # Fire and forget (or await id)
                await self._place_worker(order)

    async def _place_worker(self, order_def):
        try:
            params = {
                "symbol": self.symbol,
                "side": order_def["side"],
                "order_type": "limit",
                "qty": self.order_qty,
                "price": str(order_def["price"]),
                "time_in_force": "alo", # StandX uses 'alo' (Add Liquidity Only) for PostOnly
                "reduce_only": False
            }
            
            # Generate ID
            cl_ord_id = f"mm-{int(time.time()*1000)}-{order_def['price']}"
            params["cl_ord_id"] = cl_ord_id
            
            # Debug Log
            # logger.info(f"🚀 Sending Order: {params}")
            
            await self.ws.place_order(params)
            
            # Track it (Mapping Price -> ClientOrderID)
            # Note: cancel logic in ws_client needs to support cl_ord_id
            self.pending_orders[order_def["price"]] = {
                "id": cl_ord_id,
                "side": order_def["side"],
                "qty": self.order_qty
            }
            
        except Exception as e:
            logger.error(f"❌ Place failed for {order_def}: {e}", exc_info=True)

    def _generate_grid_prices(self, current_price):
        """
        Same grid snapping logic as before.
        """
        # Bid/Ask
        bid_raw = current_price - self.price_spread
        ask_raw = current_price + self.price_spread
        
        # Snap
        bid_base = int(bid_raw / self.price_step) * self.price_step
        ask_base = int((ask_raw + self.price_step - 1) / self.price_step) * self.price_step
        
        longs = []
        for i in range(self.grid_count):
            p = bid_base - i * self.price_step
            longs.append(p)
            
        shorts = []
        for i in range(self.grid_count):
            p = ask_base + i * self.price_step
            shorts.append(p)
            
        return longs, shorts

    async def cancel_all(self):
        """Cancel all pending orders on shutdown."""
        async with self.lock:
             to_cancel = [info["id"] for info in self.pending_orders.values()]
             if to_cancel:
                 try:
                     await self.ws.cancel_orders(to_cancel, self.symbol, is_client_id=True)
                 except Exception as e:
                     logger.error(f"Cancel failed: {e}")
