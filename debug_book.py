import asyncio
import websockets
import json
import time

WS_URL = "wss://perps.standx.com/ws-stream/v1"
SYMBOL = "BTC-USD"

async def monitor_book():
    print(f"Connecting to {WS_URL}...")
    async with websockets.connect(WS_URL) as ws:
        # Auth might be needed? 
        # ws_client says "Market Stream Auth (User Provided Format)". 
        # But public channels usually don't need auth? 
        # Let's try public subscribe first.
        
        sub_msg = {
            "subscribe": {
                "channel": "depth_book",
                "symbol": SYMBOL
            }
        }
        await ws.send(json.dumps(sub_msg))
        print(f"Subscribed to {SYMBOL} depth_book...")
        
        while True:
            msg = await ws.recv()
            data = json.loads(msg)
            
            if data.get("channel") == "depth_book":
                content = data.get("data", {})
                bids = content.get("bids", [])
                asks = content.get("asks", [])
                
                if bids and asks:
                    best_bid = float(bids[0][0])
                    best_ask = float(asks[0][0])
                    mid = (best_bid + best_ask) / 2
                    spread = best_ask - best_bid
                    
                    print(f"[{time.strftime('%H:%M:%S')}] count={len(bids)}")
                    
                    # Sort check
                    print(f"   Bids (0): {bids[0]} (Might be Lowest?)")
                    print(f"   Bids (-1): {bids[-1]} (Might be Highest/Best?)")
                    print(f"   Asks (0): {asks[0]} (Best Ask?)")
                    print(f"   Asks (-1): {asks[-1]} (Worst Ask?)")
                    
                    best_bid_candidate = float(bids[-1][0]) # Assuming Ascending
                    best_ask_candidate = float(asks[0][0])  # Assuming Ascending
                    
                    mid = (best_bid_candidate + best_ask_candidate) / 2
                    print(f"   Hypothesis Mid: {mid:.2f} (Spread: {best_ask_candidate - best_bid_candidate:.2f})")
                    print("-" * 20)
                    
            elif data.get("channel") == "auth":
                pass
            else:
                # print(f"Other msg: {data}")
                pass

if __name__ == "__main__":
    try:
        asyncio.run(monitor_book())
    except KeyboardInterrupt:
        print("Stopped.")
