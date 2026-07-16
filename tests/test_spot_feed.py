import sys
sys.path.insert(0, ".")
from spot_feed import SpotFeed


def test_window_open_and_margin():
    sf = SpotFeed()
    sf._on_price("BTC", 100.0)   # first tick of window -> becomes the open
    sf._on_price("BTC", 100.5)   # later tick -> latest
    m = sf.get_margin("BTC")
    assert m is not None
    assert m["open"] == 100.0 and m["now"] == 100.5
    assert abs(m["margin_pct"] - 0.5) < 1e-6   # +0.5% above open (favors Up)


def test_negative_margin():
    sf = SpotFeed()
    sf._on_price("ETH", 200.0)
    sf._on_price("ETH", 199.0)
    m = sf.get_margin("ETH")
    assert m["margin_pct"] < 0   # below open (favors Down)


def test_no_data_returns_none():
    sf = SpotFeed()
    assert sf.get_margin("SOL") is None


def test_window_resolution_up_down():
    sf = SpotFeed()
    # Simulate a window: open 100, ticks up to 100.5 (close >= open -> UP)
    wts = sf._window_ts()
    sf.windows["BTC"] = {wts: {"open": 100.0, "close": 100.5}}
    assert sf.get_window_resolution("BTC", wts) == "UP"
    sf.windows["ETH"] = {wts: {"open": 100.0, "close": 99.5}}
    assert sf.get_window_resolution("ETH", wts) == "DOWN"
    # tie resolves UP (rule is close >= open)
    sf.windows["SOL"] = {wts: {"open": 100.0, "close": 100.0}}
    assert sf.get_window_resolution("SOL", wts) == "UP"
    # missing window -> None
    assert sf.get_window_resolution("BTC", wts - 300) is None


if __name__ == "__main__":
    test_window_open_and_margin()
    test_negative_margin()
    test_no_data_returns_none()
    print("spot_feed tests passed")
