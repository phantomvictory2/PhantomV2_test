import urllib.request
import json
import time

def send_tick(price, delay_ms):
    url = "http://localhost:8000/api/test/inject_tick"
    timestamp_ms = int(time.time() * 1000) + delay_ms
    data = {
        "symbol": "BTCUSDT",
        "price": price,
        "timestamp_ms": timestamp_ms
    }
    req = urllib.request.Request(
        url,
        data=json.dumps(data).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST"
    )
    try:
        with urllib.request.urlopen(req) as res:
            print(f"Injected tick: {price} -> {res.read().decode('utf-8')}")
    except Exception as e:
        print(f"Failed to inject tick: {e}")

def main():
    print("===================================================")
    print("Phantom V2 E2E Mock Signal Injector")
    print("===================================================")
    print("Waiting 12 seconds for Polymarket feed to become stale (staleness >= 10.0s is required for LATENCY_ARB)...")
    time.sleep(12)
    
    print("\nInjecting initial base tick...")
    send_tick(50000.0, 0)
    
    time.sleep(1.0)
    
    print("Injecting surge tick (+0.5% price change, duration 1s)...")
    send_tick(50250.0, 1000)
    
    print("\nInjection complete! Check the terminal logs and dashboard at http://localhost:8000.")

if __name__ == "__main__":
    main()
