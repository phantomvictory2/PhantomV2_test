import asyncio
import time
import httpx
from fastapi.testclient import TestClient
from dashboard import create_app

class MockRiskEngine:
    def __init__(self):
        self.state_provider = type("SP", (), {"config": {"kill_switch": False}})()
    def get_config(self, key, default):
        return self.state_provider.config.get(key, default)

def test_dashboard():
    import os
    orig_pwd = os.environ.pop("DASHBOARD_PASSWORD", None)
    try:
        re = MockRiskEngine()
        app = create_app(risk_engine=re)
        client = TestClient(app)

        print("\n--- TEST: DASHBOARD HTML LOAD ---")
        res = client.get("/", auth=("admin", "pwd"))
        assert res.status_code == 200
        assert b"PHANTOM V2 Dashboard" in res.content
        print("HTML Dashboard loaded successfully.")

        print("\n--- TEST: API STATE RESPONSE ---")
        res = client.get("/api/state", auth=("admin", "pwd"))
        assert res.status_code == 200
        data = res.json()
        assert "strategies" in data
        assert "bankroll" in data
        print(f"API returned state with {len(data['strategies'])} strategies.")

        print("\n--- TEST: KILL SWITCH TOGGLE LATENCY (<500ms) ---")
        start_time = time.time()
        
        # Toggle ON
        res_toggle = client.post("/api/kill_switch", auth=("admin", "pwd"))
        assert res_toggle.status_code == 200
        
        # Check if Risk Engine sees the change
        is_active = re.get_config("kill_switch", False)
        
        end_time = time.time()
        latency_ms = (end_time - start_time) * 1000
        
        print(f"Kill switch toggled to: {is_active}")
        print(f"Toggle latency: {latency_ms:.2f}ms")
        
        if latency_ms < 500:
            print("[OK] Latency test passed (<500ms).")
        else:
            print("[FAIL] Latency test failed (>500ms).")
    finally:
        if orig_pwd is not None:
            os.environ["DASHBOARD_PASSWORD"] = orig_pwd

if __name__ == "__main__":
    test_dashboard()
