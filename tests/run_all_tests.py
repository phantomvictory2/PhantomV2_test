import subprocess
import os
import sys

test_files = [
    "test_spot_feed.py",
    "test_risk_guards.py",
    "test_clob_executor.py",
    "test_dashboard.py",
    "test_direction_pricing.py",
    "test_end_to_end.py",
    "test_executor.py",
    "test_monitor.py",
    "test_pending_reservation.py",
    "test_poly_feed.py",
    "test_risk_engine.py",
    "test_signal_engine.py",
    "test_strategy_stats.py",
    "test_telegram.py"
]

print("=== Running all tests sequentially ===")
failures = []

for tf in test_files:
    if not os.path.exists(tf):
        print(f"Skipping {tf} (does not exist)")
        continue
    
    print(f"\nRunning {tf}...")
    res = subprocess.run([sys.executable, tf], capture_output=True, text=True, encoding='utf-8', errors='replace')
    if res.returncode == 0:
        print(f"[PASS] {tf}")
    else:
        print(f"[FAIL] {tf} (Exit Code: {res.returncode})")
        print("--- STDOUT ---")
        print(res.stdout.encode('ascii', 'replace').decode('ascii'))
        print("--- STDERR ---")
        print(res.stderr.encode('ascii', 'replace').decode('ascii'))
        failures.append(tf)

print("\n=== Test Results Summary ===")
if failures:
    print(f"Failed tests ({len(failures)}):")
    for f in failures:
        print(f"  - {f}")
    sys.exit(1)
else:
    print("All tests passed successfully!")
    sys.exit(0)
