import time
import requests
import asyncio
import websockets
import statistics

# Configuration
WS_ENDPOINT = "wss://perps.standx.com/ws-stream/v1"
TEST_COUNT = 10

async def check_ws_latency():
    print(f"\n🔌 Testing WebSocket Connect Latency to {WS_ENDPOINT}...")
    latencies = []
    
    for i in range(TEST_COUNT):
        start = time.time()
        try:
            async with websockets.connect(WS_ENDPOINT, ping_interval=None) as ws:
                duration = (time.time() - start) * 1000
                latencies.append(duration)
                print(f"   Conn {i+1}: {duration:.2f} ms")
                # Just connect and close. Reliable metric for handshake.
        except Exception as e:
            print(f"   Conn {i+1}: ❌ Error ({e})")
            
        await asyncio.sleep(0.2)

    if latencies:
        print(f"📊 Stats: Min={min(latencies):.2f}ms | Max={max(latencies):.2f}ms | Avg={statistics.mean(latencies):.2f}ms")

def main():
    print("🚀 StandX Network Latency Tester")
    print("================================")
    asyncio.run(check_ws_latency())
    print("\n✅ Test Complete.")

if __name__ == "__main__":
    main()
