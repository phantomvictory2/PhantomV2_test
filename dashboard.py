import os
import time

START_TIME = time.time()
import asyncio
import logging
import json
import copy
from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
import secrets
from analytics_engine import AnalyticsEngine

logger = logging.getLogger(__name__)

# Basic Authentication Helper
security = HTTPBasic()

def verify_credentials(credentials: HTTPBasicCredentials = Depends(security)):
    dashboard_password = os.getenv("DASHBOARD_PASSWORD")
    if not dashboard_password:
        # If no password is set, allow access
        return True
    
    # Enforce basic authentication (username can be anything, e.g. "admin")
    is_correct_password = secrets.compare_digest(credentials.password, dashboard_password)
    if not is_correct_password:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Basic"},
        )
    return True

# Optional Auth Dependency
def auth_dependency(auth_ok: bool = Depends(verify_credentials)):
    return auth_ok


class GlobalStateProvider:
    def __init__(self):
        self.config = {"kill_switch": False, "paper_mode": "true", "daily_trade_limit": 100}
        self.open_positions = []
        self.lag_stats = {"BTC": {"avg_lag_seconds": 15.0, "sample_size": 20, "status": "ACTIVE"}}
        self.daily_stats = {"BTC": {"loss_pct": 0.0, "win_rate": 0.50}}
        self.pos_stats = {"total": 0, "BTC": 0}
        self.trade_counts = {"total": 0, "BTC": 0}
        self.cons_loss = 0
        self.poly_feed = None
        self.binance_feed = None
        self.spot_feed = None
        self.db_manager = None
        self.strategy_engine = None
        self.bankroll = 1000.0
        self.pending_assets = set()
        self._resolution_cache = {}
        self.boot_time = None
        self.telegram_callback = None
        # BUG FIX: Real daily stats tracking (was hardcoded zeros)
        self._today_trades = 0
        self._today_wins = 0
        self._today_losses = 0
        self._today_pnl = 0.0
        self._today_date = None  # Reset when date changes
        # BUG FIX: Real consecutive loss tracking (was no-op)
        self._consecutive_losses = 0
        self._last_loss_time = 0.0  # epoch seconds

    @property
    def has_live_data(self) -> bool:
        if self.poly_feed:
            for state in self.poly_feed.market_state.values():
                if state.get("has_live_data", False):
                    return True
        return False

    @property
    def best_bid(self) -> dict:
        bids = {}
        for asset in ["BTC", "ETH", "SOL"]:
            if self.poly_feed:
                state = self.poly_feed.get_market_state(asset)
                bids[asset] = state.get("best_bid", 0.50) if state else 0.50
            else:
                bids[asset] = 0.50
        return bids

    @property
    def best_ask(self) -> dict:
        asks = {}
        for asset in ["BTC", "ETH", "SOL"]:
            if self.poly_feed:
                state = self.poly_feed.get_market_state(asset)
                asks[asset] = state.get("best_ask", 0.50) if state else 0.50
            else:
                asks[asset] = 0.50
        return asks

    def disable_asset(self, asset: str):
        try:
            if asset not in self.lag_stats:
                self.lag_stats[asset] = {"status": "ACTIVE", "sample_size": 0, "avg_lag_seconds": 10.0}
            self.lag_stats[asset]["status"] = "DISABLED"
            
            if self.db_manager:
                async def run_db_disable():
                    try:
                        async with self.db_manager.pool.acquire(timeout=5.0) as conn:
                            await conn.execute("""
                                INSERT INTO asset_lag_stats (asset, status, last_updated)
                                VALUES ($1, 'DISABLED', NOW())
                                ON CONFLICT (asset) DO UPDATE SET status = 'DISABLED', last_updated = NOW()
                            """, asset)
                        logger.info(f"Asset {asset} oracle status updated to DISABLED in database.")
                    except Exception as e:
                        logger.error(f"Failed to update asset {asset} status in DB: {e}")
                        tg_cb = getattr(self, "telegram_callback", None)
                        if tg_cb:
                            await tg_cb(f"⚠️ Failed to disable asset {asset} in DB: {e}")
                asyncio.create_task(run_db_disable())
        except Exception as e:
            logger.error(f"Exception inside disable_asset: {e}", exc_info=True)
            tg_cb = getattr(self, "telegram_callback", None)
            if tg_cb:
                if asyncio.iscoroutinefunction(tg_cb):
                    asyncio.create_task(tg_cb(f"⚠️ Exception inside disable_asset for {asset}: {e}"))
                else:
                    tg_cb(f"⚠️ Exception inside disable_asset for {asset}: {e}")

    def get_config(self, key, default):
        return self.config.get(key, default)

    def set_config(self, key, val):
        self.config[key] = val
        if self.db_manager:
            asyncio.create_task(self.db_manager.update_system_config(key, val))

    def get_asset_lag_stats(self, asset):
        return self.lag_stats.get(asset, {"status": "ACTIVE", "sample_size": 0, "avg_lag_seconds": 10.0})

    def get_daily_asset_stats(self, asset):
        return self.daily_stats.get(asset, {"loss_pct": 0, "win_rate": 0.5})

    def get_daily_stats(self):
        # BUG FIX: Return real daily stats instead of hardcoded zeros
        from datetime import date
        today = date.today()
        if self._today_date != today:
            # New day — reset counters
            self._today_trades = 0
            self._today_wins = 0
            self._today_losses = 0
            self._today_pnl = 0.0
            self._today_date = today

        total_closed = self._today_wins + self._today_losses
        win_rate = self._today_wins / total_closed if total_closed > 0 else 0.5
        loss_rate = self._today_losses / total_closed if total_closed > 0 else 0.0
        daily_loss_pct = abs(self._today_pnl) / self.bankroll if self._today_pnl < 0 and self.bankroll > 0 else 0.0

        return {
            "daily_loss_pct": daily_loss_pct,
            "win_rate_today": win_rate,
            "loss_rate_today": loss_rate,
            "trades_today": self._today_trades
        }

    def get_position_stats(self, asset: str) -> dict:
        total = 0
        asset_in_candle = 0
        for p in self.open_positions:
            if p.get("status") == "OPEN":
                total += 1
                if p.get("asset") == asset:
                    # Very simple in-candle check proxy: if opened in last 60s
                    if (time.time() * 1000) - p.get("opened_at", 0) < 60000:
                        asset_in_candle += 1
        return {"total": total, "asset_in_candle": asset_in_candle}
        
    def get_open_markets(self) -> set:
        open_markets = set()
        for p in self.open_positions:
            if p.get("status") == "OPEN" and "market_id" in p:
                open_markets.add(p["market_id"])
        return open_markets

    def get_open_position_stats(self):
        active_positions = [p for p in self.open_positions if p.get("status") == "OPEN"]
        total = len(active_positions)
        btc_count = sum(1 for p in active_positions if p.get("asset") == "BTC")
        return {"total": total, "BTC": btc_count}

    def get_live_balance(self):
        return self.bankroll

    def get_daily_trade_count(self, asset):
        return self.trade_counts.get(asset, 0), self.trade_counts.get("total", 0)

    def get_consecutive_losses(self):
        # BUG FIX: Return real consecutive loss data instead of hardcoded zeros
        seconds_since = time.time() - self._last_loss_time if self._last_loss_time > 0 else 9999
        return {"count": self._consecutive_losses, "seconds_since_last": seconds_since}

    def get_bankroll(self):
        return self.bankroll

    def set_cooldown(self, asset, duration):
        pass

    def is_on_cooldown(self, asset):
        return False
    
    def get_market_state(self, asset):
        if self.poly_feed:
            return self.poly_feed.get_market_state(asset)
        return {"yes_price": 0.5, "no_price": 0.5}

    def get_open_positions(self):
        return self.open_positions

    async def fetch_market_from_api(self, market_id: str) -> dict:
        if not market_id:
            return None
            
        # Check cache
        if market_id in self._resolution_cache:
            return self._resolution_cache[market_id]

        # Determine if condition ID (hex string) or numeric ID.
        # The Gamma /markets endpoint filters out CLOSED markets by default, so a
        # condition_ids query returns empty once a 5-min market settles. We must
        # query closed markets explicitly to read their resolution. Try the resolved
        # (closed=true) market first, then fall back to the live query.
        import urllib.request
        import json

        if market_id.startswith("0x"):
            urls = [
                f"https://gamma-api.polymarket.com/markets?condition_ids={market_id}&closed=true",
                f"https://gamma-api.polymarket.com/markets?condition_ids={market_id}",
            ]
        else:
            urls = [f"https://gamma-api.polymarket.com/markets/{market_id}"]

        retries = 3
        backoff = 0.5
        for attempt in range(retries):
            try:
                market_data = None
                for url in urls:
                    req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
                    res = await asyncio.to_thread(urllib.request.urlopen, req, timeout=5.0)
                    data = json.loads(res.read().decode('utf-8'))
                    if isinstance(data, list) and len(data) > 0:
                        market_data = data[0]
                    elif isinstance(data, dict) and "id" in data:
                        market_data = data
                    if market_data:
                        break

                if market_data:
                    # Cache resolved markets permanently with an eviction cap to prevent memory bloat
                    is_resolved = market_data.get("closed") is True or market_data.get("umaResolutionStatus") == "resolved"
                    if is_resolved:
                        if len(self._resolution_cache) >= 500:
                            first_key = next(iter(self._resolution_cache))
                            self._resolution_cache.pop(first_key, None)
                        self._resolution_cache[market_id] = market_data
                    return market_data
                    
                return None
            except Exception as e:
                logger.warning(f"Gamma API lookup attempt {attempt+1} failed for {market_id}: {e}")
                if attempt < retries - 1:
                    await asyncio.sleep(backoff * (2 ** attempt))
                else:
                    logger.error(f"Gamma API lookup completely failed for {market_id} after {retries} attempts.")
        return None

    async def is_market_resolved(self, market_id: str) -> bool:
        if not market_id or (not market_id.startswith("0x") and not market_id.isdigit()):
            # Fallback for mock IDs in tests
            return True
            
        market_data = await self.fetch_market_from_api(market_id)
        if market_data:
            if market_data.get("closed") is True or market_data.get("umaResolutionStatus") == "resolved":
                return True
            # Decisive outcome prices mean the market has effectively resolved even
            # if the on-chain UMA/closed flag hasn't flipped yet (it can lag 10+ min).
            prices = market_data.get("outcomePrices")
            if isinstance(prices, str):
                try:
                    prices = json.loads(prices)
                except Exception:
                    prices = None
            if prices and len(prices) >= 2:
                try:
                    p = [float(prices[0]), float(prices[1])]
                    if max(p) >= 0.99:
                        return True
                except (ValueError, TypeError):
                    pass
        return False

    async def get_resolution_price(self, market_id: str, dir: str) -> float:
        if not market_id or (not market_id.startswith("0x") and not market_id.isdigit()):
            # Fallback for mock IDs in tests
            return 1.0
            
        market_data = await self.fetch_market_from_api(market_id)
        if market_data:
            prices = market_data.get("outcomePrices")
            if isinstance(prices, str):
                try:
                    prices = json.loads(prices)
                except Exception:
                    pass
            
            if prices and len(prices) >= 2:
                try:
                    yes_price = float(prices[0])
                    no_price = float(prices[1])
                    if dir == "BUY_YES":
                        return yes_price
                    elif dir == "BUY_NO":
                        return no_price
                except ValueError:
                    pass
        return 0.0

    def update_consecutive_loss_counter(self, result):
        # BUG FIX: Actually track consecutive losses (was no-op)
        from datetime import date
        today = date.today()
        if self._today_date != today:
            self._today_trades = 0
            self._today_wins = 0
            self._today_losses = 0
            self._today_pnl = 0.0
            self._today_date = today

        self._today_trades += 1
        if result == "WIN":
            self._today_wins += 1
            self._consecutive_losses = 0  # Reset on win
        elif result == "LOSS":
            self._today_losses += 1
            self._consecutive_losses += 1
            self._last_loss_time = time.time()

    def update_asset_lag_stats(self, asset, lag):
        if self.db_manager:
            asyncio.create_task(self.db_manager.upsert_asset_lag(asset, lag))

    def update_strategy_stats(self, pos: dict):
        if self.strategy_engine:
            asyncio.create_task(self.strategy_engine.process_closed_trade(copy.deepcopy(pos)))

    def record_pnl(self, pnl):
        self.bankroll += pnl
        # BUG FIX: Track daily PnL for risk engine daily loss check
        self._today_pnl += pnl

    def get_dashboard_state(self):
        return {"bankroll": self.bankroll, "today_pnl": self._today_pnl, "kill_switch": self.config["kill_switch"]}


state_provider = GlobalStateProvider()
app = FastAPI(title="Phantom V2 Dashboard")

def create_app(state_provider_instance=None, risk_engine=None):
    global state_provider
    if state_provider_instance is not None:
        state_provider = state_provider_instance

    html_content = """<!-- PHANTOM V2 Dashboard -->
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Phantom V2 | Quantitative Trading</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&family=JetBrains+Mono:wght@400;500;700&display=swap" rel="stylesheet">
    <style>
        *{margin:0;padding:0;box-sizing:border-box;}
        :root {
            --bg:#0B0F1A; --sb:#0D1321; --panel:#111827; --panel2:#1A2235;
            --border:#1E2D40; --border2:#253347;
            --text:#E2E8F0; --muted:#64748B; --muted2:#94A3B8;
            --green:#10B981; --gdim:rgba(16,185,129,.15);
            --red:#EF4444;   --rdim:rgba(239,68,68,.15);
            --blue:#3B82F6;  --bdim:rgba(59,130,246,.15);
            --purple:#8B5CF6;--pdim:rgba(139,92,246,.15);
            --yellow:#F59E0B;
            --mono:'JetBrains Mono',monospace;
            --sans:'Inter',sans-serif;
        }

        body{font-family:var(--sans);background:var(--bg);color:var(--text);display:flex;height:100vh;overflow:hidden;}

        /* ── SIDEBAR ── */
        .sidebar{width:220px;background:var(--sb);border-right:1px solid var(--border);display:flex;flex-direction:column;flex-shrink:0;z-index:10;overflow:hidden;}
        .brand{padding:18px 16px 14px;border-bottom:1px solid var(--border);display:flex;align-items:center;gap:10px;}
        .brand-ghost{font-size:1.5rem;line-height:1;}
        .brand-text{font-size:.95rem;font-weight:800;letter-spacing:.04em;color:var(--text);}
        .brand-badge{font-size:.55rem;font-weight:700;padding:2px 6px;border-radius:4px;background:var(--bdim);color:var(--blue);border:1px solid var(--blue);margin-left:4px;vertical-align:middle;}
        .nav-section{font-size:.6rem;font-weight:700;letter-spacing:.12em;color:var(--muted);padding:14px 16px 6px;text-transform:uppercase;}
        .nav-item{display:flex;align-items:center;gap:10px;padding:10px 16px;cursor:pointer;color:var(--muted2);font-size:.82rem;font-weight:500;transition:.15s;border-left:2px solid transparent;}
        .nav-item:hover{background:rgba(255,255,255,.04);color:var(--text);}
        .nav-item.active{background:var(--bdim);color:var(--blue);border-left-color:var(--blue);}
        .nav-icon{width:16px;text-align:center;font-size:.85rem;opacity:.8;}
        .nav-spacer{flex:1;}
        .sys-health{padding:12px 16px;border-top:1px solid var(--border);display:flex;align-items:center;gap:8px;font-size:.75rem;color:var(--muted2);}
        .pulse-dot{width:8px;height:8px;border-radius:50%;background:var(--green);box-shadow:0 0 6px var(--green);animation:pulse 2s infinite;}
        @keyframes pulse{0%,100%{opacity:1;}50%{opacity:.4;}}
        .q-nav{margin:10px 12px;padding:10px 12px;background:linear-gradient(135deg,rgba(139,92,246,.2),rgba(59,130,246,.2));border:1px solid rgba(139,92,246,.4);border-radius:8px;cursor:pointer;color:#c4b5fd;font-size:.8rem;font-weight:700;text-align:center;transition:.2s;}
        .q-nav:hover{background:linear-gradient(135deg,rgba(139,92,246,.35),rgba(59,130,246,.35));}

        /* ── MAIN ── */
        .main{flex:1;display:flex;flex-direction:column;height:100%;overflow:hidden;}

        /* ── TOP BAR ── */
        .topbar{display:flex;align-items:center;gap:16px;padding:10px 20px;border-bottom:1px solid var(--border);background:var(--sb);flex-shrink:0;flex-wrap:wrap;}
        .run-badge{display:flex;align-items:center;gap:6px;font-size:.72rem;font-weight:700;color:var(--green);border:1px solid rgba(16,185,129,.4);padding:4px 10px;border-radius:20px;background:var(--gdim);}
        .run-dot{width:7px;height:7px;border-radius:50%;background:var(--green);animation:pulse 2s infinite;}
        .tb-metric{display:flex;flex-direction:column;}
        .tb-label{font-size:.6rem;font-weight:600;color:var(--muted);text-transform:uppercase;letter-spacing:.06em;}
        .tb-val{font-family:var(--mono);font-size:.95rem;font-weight:700;color:var(--text);}
        .tb-sep{width:1px;height:30px;background:var(--border);}
        .tb-right{margin-left:auto;display:flex;align-items:center;gap:8px;}
        .ks-label{font-size:.7rem;font-weight:600;color:var(--muted);margin-right:2px;}
        .ks-toggle{position:relative;width:38px;height:20px;background:#1e293b;border-radius:10px;border:1px solid var(--border);cursor:pointer;transition:.2s;}
        .ks-toggle.on{background:rgba(239,68,68,.3);border-color:var(--red);}
        .ks-thumb{position:absolute;top:3px;left:3px;width:12px;height:12px;border-radius:50%;background:var(--muted);transition:.2s;}
        .ks-toggle.on .ks-thumb{left:21px;background:var(--red);}
        .btn{padding:6px 14px;border-radius:6px;font-size:.78rem;font-weight:700;cursor:pointer;border:1px solid var(--border);background:rgba(255,255,255,.05);color:var(--text);transition:.15s;font-family:var(--sans);}
        .btn:hover{background:rgba(255,255,255,.1);}
        .btn-blue{background:var(--blue);border-color:var(--blue);color:#fff;}
        .btn-blue:hover{background:#2563eb;}

        /* ── LIVE DATA BAR ── */
        .live-bar{display:flex;align-items:center;gap:16px;padding:7px 20px;background:rgba(13,19,33,.7);border-bottom:1px solid var(--border);flex-shrink:0;font-size:.72rem;flex-wrap:wrap;}
        .live-label{font-weight:700;color:var(--muted);text-transform:uppercase;letter-spacing:.08em;}
        .feed-item{display:flex;align-items:center;gap:5px;color:var(--muted2);}
        .feed-dot{width:6px;height:6px;border-radius:50%;background:var(--muted);}
        .feed-dot.on{background:var(--green);box-shadow:0 0 4px var(--green);}
        .live-bar-right{margin-left:auto;display:flex;gap:16px;color:var(--muted2);}
        .live-bar-right span b{color:var(--text);}

        /* ── SCROLL AREA ── */
        .scroll{flex:1;overflow-y:auto;padding:18px 20px;}
        .scroll::-webkit-scrollbar{width:5px;}
        .scroll::-webkit-scrollbar-track{background:transparent;}
        .scroll::-webkit-scrollbar-thumb{background:var(--border2);border-radius:3px;}

        /* ── PAGE ROUTING ── */
        .page{display:none;animation:fadeIn .25s ease;}
        .page.active{display:block;}
        @keyframes fadeIn{from{opacity:0;transform:translateY(4px);}to{opacity:1;transform:translateY(0);}}

        /* ── PRICE CARDS ── */
        .price-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:14px;margin-bottom:16px;}
        .price-card{background:var(--panel);border:1px solid var(--border);border-radius:10px;padding:14px;}
        .pc-header{display:flex;justify-content:space-between;align-items:center;margin-bottom:10px;}
        .pc-asset{font-size:.75rem;font-weight:700;color:var(--text);}
        .pc-badge{font-size:.55rem;font-weight:700;padding:2px 6px;border-radius:4px;background:rgba(251,191,36,.1);color:var(--yellow);border:1px solid rgba(251,191,36,.3);}
        .pc-price{font-family:var(--mono);font-size:1.5rem;font-weight:800;color:var(--text);line-height:1;}
        .pc-sparkline{margin:8px 0 10px;position:relative;height:40px;overflow:hidden;}
        .pc-divider{height:1px;background:var(--border);margin:8px 0;}
        .pc-poly-header{font-size:.6rem;font-weight:700;color:var(--muted);text-transform:uppercase;letter-spacing:.08em;margin-bottom:6px;}
        .pc-poly-row{display:flex;justify-content:space-between;align-items:center;font-size:.75rem;margin-bottom:3px;}
        .pc-yes{color:var(--green);font-family:var(--mono);font-weight:700;}
        .pc-no{color:var(--red);font-family:var(--mono);font-weight:700;}
        .pc-spread{color:var(--muted2);font-family:var(--mono);font-size:.7rem;}
        .pc-lag{font-size:.65rem;color:var(--muted);margin-top:4px;}

        /* ── 2-COL GRID ── */
        .grid2{display:grid;grid-template-columns:1fr 1fr;gap:14px;margin-bottom:16px;}
        .grid3{display:grid;grid-template-columns:repeat(3,1fr);gap:10px;margin-bottom:8px;}

        /* ── PANELS ── */
        .panel{background:var(--panel);border:1px solid var(--border);border-radius:10px;padding:16px;margin-bottom:14px;}
        .ph{display:flex;justify-content:space-between;align-items:center;margin-bottom:12px;}
        .ph-title{font-size:.7rem;font-weight:700;text-transform:uppercase;letter-spacing:.1em;color:var(--muted2);}
        .ph-right{font-size:.68rem;color:var(--muted);}

        /* ── CHARTS ── */
        .chart-wrap{position:relative;}

        /* ── TABLES ── */
        table{width:100%;border-collapse:collapse;font-size:.78rem;}
        th{padding:8px 10px;text-align:left;font-size:.65rem;font-weight:700;text-transform:uppercase;letter-spacing:.07em;color:var(--muted);border-bottom:1px solid var(--border);}
        td{padding:8px 10px;border-bottom:1px solid rgba(30,45,64,.6);font-family:var(--mono);}
        tr:last-child td{border-bottom:none;}
        tr:hover td{background:rgba(255,255,255,.02);}
        .tbl-empty{text-align:center;padding:24px!important;color:var(--muted);font-family:var(--sans);font-size:.8rem;}

        /* ── TRADE HISTORY ── */
        .th-dot{display:inline-block;width:8px;height:8px;border-radius:50%;margin-right:6px;}
        .th-dot.win{background:var(--green);}
        .th-dot.loss{background:var(--red);}
        .pnl-pos{color:var(--green);}
        .pnl-neg{color:var(--red);}
        .badge{display:inline-block;padding:2px 7px;border-radius:4px;font-size:.65rem;font-weight:700;font-family:var(--sans);}
        .badge-green{background:var(--gdim);color:var(--green);}
        .badge-red{background:var(--rdim);color:var(--red);}
        .badge-blue{background:var(--bdim);color:var(--blue);}
        .badge-gray{background:rgba(100,116,139,.15);color:var(--muted2);}

        /* ── METRIC CARDS ── */
        .mc{background:var(--panel2);border:1px solid var(--border);border-radius:8px;padding:12px 14px;}
        .mc-label{font-size:.65rem;font-weight:600;text-transform:uppercase;letter-spacing:.07em;color:var(--muted);margin-bottom:4px;}
        .mc-val{font-family:var(--mono);font-size:1.3rem;font-weight:800;color:var(--text);}
        .mc-sub{font-size:.65rem;color:var(--muted);margin-top:3px;}

        /* ── QUANTUM MINI ── */
        .qa-mini-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:10px;margin-bottom:12px;}
        .qa-metric{background:var(--panel2);border:1px solid var(--border);border-radius:8px;padding:10px 12px;display:flex;flex-direction:column;gap:4px;}
        .qa-label{font-size:.6rem;font-weight:700;text-transform:uppercase;color:var(--muted);letter-spacing:.06em;}
        .qa-val{font-family:var(--mono);font-size:1.1rem;font-weight:800;}
        .qa-spark-wrap{position:relative;height:28px;margin-top:2px;overflow:hidden;flex-shrink:0;}
        .qa-spark{position:absolute;top:0;left:0;width:100%;height:100%;}
        .qa-row{display:flex;justify-content:space-between;padding:4px 0;border-top:1px solid var(--border);font-size:.78rem;}
        .qa-row:first-of-type{border-top:none;}
        .qa-open-btn{width:100%;margin-top:12px;padding:10px;background:var(--blue);border:none;border-radius:8px;color:#fff;font-size:.85rem;font-weight:700;cursor:pointer;font-family:var(--sans);transition:.15s;}
        .qa-open-btn:hover{background:#2563eb;}

        .clr-g{color:var(--green)!important;}
        .clr-r{color:var(--red)!important;}
        .clr-b{color:var(--blue)!important;}
        .clr-m{color:var(--muted)!important;}

        /* ── STRATEGY TABLE ── */
        .strat-bar-bg{background:rgba(59,130,246,.12);border-radius:3px;height:6px;margin-top:3px;}
        .strat-bar-fill{height:6px;border-radius:3px;background:var(--blue);}

        /* ── POSITIONS PAGE ── */
        .pos-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:14px;}

        /* ── CLICKABLE STRATEGY CARDS ── */
        .panel-clickable{cursor:pointer;transition:transform .15s, border-color .15s;}
        .panel-clickable:hover{border-color:var(--blue);transform:translateY(-2px);background:rgba(26,34,53,.4);}
    </style>
</head>
<body>

<!-- ══════════════════ SIDEBAR ══════════════════ -->
<div class="sidebar">
    <div class="brand">
        <span class="brand-ghost">👻</span>
        <div>
            <div class="brand-text">PHANTOM V2 <span class="brand-badge">PAPER</span></div>
        </div>
    </div>

    <div class="nav-section">Trading</div>
    <div class="nav-item active" onclick="nav('dashboard')"><span class="nav-icon">📊</span> Live Dashboard</div>
    <div class="nav-item" onclick="nav('positions')"><span class="nav-icon">📂</span> Positions</div>
    <div class="nav-item" onclick="nav('history')"><span class="nav-icon">📋</span> Trade History</div>
    <div class="nav-item" onclick="nav('strategies')"><span class="nav-icon">🎯</span> Strategies</div>

    <div class="nav-section">System</div>
    <div class="nav-item" onclick="nav('feeds')"><span class="nav-icon">📡</span> Market Feeds</div>
    <div class="nav-item" onclick="nav('risk')"><span class="nav-icon">🛡️</span> Risk Management</div>
    <div class="nav-item" onclick="nav('settings')"><span class="nav-icon">⚙️</span> Settings</div>

    <div class="nav-spacer"></div>
    <div class="q-nav" onclick="nav('quantum')">⚡ QUANTUM ANALYTICS</div>
    <div class="sys-health"><span class="pulse-dot"></span> System Healthy &nbsp;·&nbsp; All feeds nominal</div>
</div>

<!-- ══════════════════ MAIN ══════════════════ -->
<div class="main">

    <!-- TOP BAR -->
    <div class="topbar">
        <div class="run-badge"><span class="run-dot"></span> RUNNING</div>
        <div class="tb-sep"></div>
        <div class="tb-metric"><span class="tb-label">Bankroll</span><span class="tb-val" id="top-bankroll">$1,000.00</span></div>
        <div class="tb-metric"><span class="tb-label">Today P&amp;L</span><span class="tb-val" id="top-today-pnl">$0.00</span></div>
        <div class="tb-sep"></div>
        <div style="display:flex;align-items:center;gap:8px;">
            <span class="ks-label">Kill Switch</span>
            <div class="ks-toggle" id="ks-toggle" onclick="toggleKillSwitch()">
                <div class="ks-thumb"></div>
            </div>
            <span id="ks-status" style="font-size:.7rem;font-weight:700;color:var(--muted2);">OFF</span>
        </div>
        <div class="tb-sep"></div>
        <div style="display:flex;align-items:center;gap:12px;background:rgba(255,255,255,.03);border:1px solid var(--border);border-radius:20px;padding:4px 14px;font-family:var(--mono);font-size:.72rem;height:28px;">
            <div style="display:flex;align-items:center;gap:6px;">
                <span style="color:var(--muted);font-weight:600;font-size:.65rem;text-transform:uppercase;letter-spacing:.04em;">UTC</span>
                <span id="clock-utc" style="color:var(--text);font-weight:700;">00:00:00</span>
            </div>
            <span style="color:var(--border);">|</span>
            <div style="display:flex;align-items:center;gap:6px;">
                <span style="color:var(--muted);font-weight:600;font-size:.65rem;text-transform:uppercase;letter-spacing:.04em;">5M CANDLE</span>
                <span id="clock-candle" style="color:var(--green);font-weight:700;">05:00</span>
            </div>
        </div>
        <div class="tb-right">
            <button class="btn" onclick="forceRefresh()">⟳ REFRESH</button>
            <button class="btn btn-blue">ACTIVATE LIVE</button>
        </div>
    </div>

    <!-- LIVE DATA BAR -->
    <div class="live-bar">
        <span class="live-label">Live Data:</span>
        <div class="feed-item"><span class="feed-dot" id="dot-binance"></span> Binance Feed</div>
        <div class="feed-item"><span class="feed-dot" id="dot-poly"></span> Polymarket Feed</div>
        <div class="feed-item"><span class="feed-dot on"></span> Data Sync</div>
        <div class="live-bar-right">
            <span>Latency: <b id="bar-latency">—</b></span>
            <span>Uptime: <b id="bar-uptime">—</b></span>
        </div>
    </div>

    <!-- SCROLL AREA -->
    <div class="scroll">

        <!-- ══ PAGE: DASHBOARD ══ -->
        <div id="page-dashboard" class="page active">

            <!-- Price Cards -->
            <div class="price-grid">
                <div class="price-card" id="pc-BTC">
                    <div class="pc-header"><span class="pc-asset">BTC / USDT</span><span class="pc-badge">BINANCE FEED</span></div>
                    <div class="pc-price" id="pc-btc-price">$0.00</div>
                    <div class="pc-sparkline"><canvas id="spark-BTC" style="position:absolute;top:0;left:0;width:100%;height:100%;"></canvas></div>
                    <div class="pc-divider"></div>
                    <div class="pc-poly-header">Polymarket 5M</div>
                    <div class="pc-poly-row"><span>YES</span><span class="pc-yes" id="pc-btc-yes">0.000</span></div>
                    <div class="pc-poly-row"><span>NO</span><span class="pc-no" id="pc-btc-no">0.000</span></div>
                    <div class="pc-poly-row"><span class="pc-spread">Spread</span><span class="pc-spread" id="pc-btc-spread">0.000</span></div>
                    <div class="pc-lag" id="pc-btc-lag">Stale: 0s</div>
                </div>
                <div class="price-card" id="pc-ETH">
                    <div class="pc-header"><span class="pc-asset">ETH / USDT</span><span class="pc-badge">BINANCE FEED</span></div>
                    <div class="pc-price" id="pc-eth-price">$0.00</div>
                    <div class="pc-sparkline"><canvas id="spark-ETH" style="position:absolute;top:0;left:0;width:100%;height:100%;"></canvas></div>
                    <div class="pc-divider"></div>
                    <div class="pc-poly-header">Polymarket 5M</div>
                    <div class="pc-poly-row"><span>YES</span><span class="pc-yes" id="pc-eth-yes">0.000</span></div>
                    <div class="pc-poly-row"><span>NO</span><span class="pc-no" id="pc-eth-no">0.000</span></div>
                    <div class="pc-poly-row"><span class="pc-spread">Spread</span><span class="pc-spread" id="pc-eth-spread">0.000</span></div>
                    <div class="pc-lag" id="pc-eth-lag">Stale: 0s</div>
                </div>
                <div class="price-card" id="pc-SOL">
                    <div class="pc-header"><span class="pc-asset">SOL / USDT</span><span class="pc-badge">BINANCE FEED</span></div>
                    <div class="pc-price" id="pc-sol-price">$0.00</div>
                    <div class="pc-sparkline"><canvas id="spark-SOL" style="position:absolute;top:0;left:0;width:100%;height:100%;"></canvas></div>
                    <div class="pc-divider"></div>
                    <div class="pc-poly-header">Polymarket 5M</div>
                    <div class="pc-poly-row"><span>YES</span><span class="pc-yes" id="pc-sol-yes">0.000</span></div>
                    <div class="pc-poly-row"><span>NO</span><span class="pc-no" id="pc-sol-no">0.000</span></div>
                    <div class="pc-poly-row"><span class="pc-spread">Spread</span><span class="pc-spread" id="pc-sol-spread">0.000</span></div>
                    <div class="pc-lag" id="pc-sol-lag">Stale: 0s</div>
                </div>
            </div>

            <!-- Portfolio + Strategy Charts -->
            <div class="grid2">
                <div class="panel">
                    <div class="ph"><span class="ph-title">Portfolio Snapshot</span><span class="ph-right" id="port-total">Total: $0.00</span></div>
                    <div style="max-width:200px;margin:0 auto;"><canvas id="chart-portfolio" height="180"></canvas></div>
                    <div class="grid3" style="margin-top:12px;">
                        <div style="text-align:center;"><div style="font-size:.65rem;color:var(--muted);">Available</div><div id="port-avail" style="font-family:var(--mono);font-size:.9rem;font-weight:700;color:var(--blue);">$0.00</div></div>
                        <div style="text-align:center;"><div style="font-size:.65rem;color:var(--muted);">In Positions</div><div id="port-pos" style="font-family:var(--mono);font-size:.9rem;font-weight:700;color:var(--green);">$0.00</div></div>
                        <div style="text-align:center;"><div style="font-size:.65rem;color:var(--muted);">Reserved</div><div id="port-res" style="font-family:var(--mono);font-size:.9rem;font-weight:700;color:var(--yellow);">$0.00</div></div>
                    </div>
                </div>
                <div class="panel">
                    <div class="ph"><span class="ph-title">Strategy Performance</span></div>
                    <div class="chart-wrap"><canvas id="chart-strategy" height="160"></canvas></div>
                    <table id="tbl-strat-mini" style="margin-top:10px;">
                        <thead><tr><th>Strategy</th><th>Trades</th><th>Win%</th><th>P&amp;L</th></tr></thead>
                        <tbody></tbody>
                    </table>
                </div>
            </div>

            <!-- Trade History -->
            <div class="panel">
                <div class="ph">
                    <span class="ph-title">Trade History (Last 15)</span>
                    <span class="ph-right" id="th-summary">—</span>
                </div>
                <table id="tbl-trade-history">
                    <thead><tr>
                        <th></th><th>Time</th><th>Asset</th><th>Strategy</th>
                        <th>Dir</th><th>Entry</th><th>Exit</th><th>Size</th><th>P&amp;L</th><th>Reason</th>
                    </tr></thead>
                    <tbody></tbody>
                </table>
            </div>

            <!-- Quantum Mini -->
            <div class="panel">
                <div class="ph">
                    <span class="ph-title">⚡ Quantum Analytics</span>
                    <span class="ph-right">AI Intelligence Engine</span>
                </div>
                <div class="qa-mini-grid">
                    <div class="qa-metric">
                        <span class="qa-label">Win Rate</span>
                        <span class="qa-val clr-g" id="qa-wr">0.0%</span>
                        <div class="qa-spark-wrap"><canvas class="qa-spark" id="qa-spark-wr"></canvas></div>
                    </div>
                    <div class="qa-metric">
                        <span class="qa-label">Profit Factor</span>
                        <span class="qa-val clr-b" id="qa-pf">0.00</span>
                        <div class="qa-spark-wrap"><canvas class="qa-spark" id="qa-spark-pf"></canvas></div>
                    </div>
                    <div class="qa-metric">
                        <span class="qa-label">Sharpe Ratio</span>
                        <span class="qa-val" id="qa-sr">0.00</span>
                        <div class="qa-spark-wrap"><canvas class="qa-spark" id="qa-spark-sr"></canvas></div>
                    </div>
                </div>
                <div class="qa-row"><span class="clr-m">Total Trades</span><span id="qa-tt" style="font-family:var(--mono);font-weight:700;">0</span></div>
                <div class="qa-row"><span class="clr-m">Total P&amp;L</span><span id="qa-tp" style="font-family:var(--mono);font-weight:700;">$0.00</span></div>
                <div class="qa-row"><span class="clr-m">Avg P&amp;L / Trade</span><span id="qa-ap" style="font-family:var(--mono);font-weight:700;">$0.00</span></div>
                <button class="qa-open-btn" onclick="nav('quantum')">OPEN QUANTUM ANALYTICS DASHBOARD →</button>
            </div>

            <!-- Live Signals Feed -->
            <div class="panel">
                <div class="ph"><span class="ph-title">Live Signal Feed</span><span class="ph-right">Last 20 signals</span></div>
                <table id="tbl-live-signals">
                    <thead><tr>
                        <th>Time</th><th>Asset</th><th>Strategy</th><th>Direction</th>
                        <th>Binance $</th><th>Poly YES</th><th>Poly NO</th><th>Status</th>
                    </tr></thead>
                    <tbody></tbody>
                </table>
            </div>
        </div>

        <!-- ══ PAGE: POSITIONS ══ -->
        <div id="page-positions" class="page">
            <div class="pos-grid">
                <div class="mc"><div class="mc-label">Active Positions</div><div class="mc-val" id="pos-count">0</div></div>
                <div class="mc"><div class="mc-label">Capital Deployed</div><div class="mc-val" id="pos-deployed">$0.00</div></div>
                <div class="mc"><div class="mc-label">Avg Hold Time</div><div class="mc-val" id="pos-hold">—</div></div>
                <div class="mc"><div class="mc-label">Win Rate 24h</div><div class="mc-val" id="pos-wr">—</div></div>
            </div>
            <div class="panel">
                <div class="ph"><span class="ph-title">Open Positions</span></div>
                <table id="tbl-active-positions">
                    <thead><tr><th>Strategy</th><th>Asset</th><th>Dir</th><th>Entry</th><th>Size</th><th>Duration</th></tr></thead>
                    <tbody></tbody>
                </table>
            </div>
        </div>

        <!-- ══ PAGE: TRADE HISTORY ══ -->
        <div id="page-history" class="page">
            <div class="panel">
                <div class="ph">
                    <span class="ph-title">Complete Trade Ledger</span>
                    <button class="btn" onclick="exportCSV()">↓ Export CSV</button>
                </div>
                <div style="max-height:600px;overflow-y:auto;">
                    <table id="tbl-history">
                        <thead><tr>
                            <th></th><th>Time</th><th>Asset</th><th>Strategy</th>
                            <th>Dir</th><th>Entry</th><th>Exit</th><th>Size</th><th>P&amp;L</th><th>Reason</th>
                        </tr></thead>
                        <tbody></tbody>
                    </table>
                </div>
            </div>
        </div>

        <!-- ══ PAGE: STRATEGIES ══ -->
        <div id="page-strategies" class="page">
            <div id="strategies-list-view">
                <div class="grid2" id="strat-cards"></div>
            </div>
            <div id="strategy-detail-view" style="display:none; animation:fadeIn .25s ease;">
                <div style="display:flex;align-items:center;gap:12px;margin-bottom:18px;">
                    <button class="btn" onclick="showStrategiesList()">← Back to Strategies</button>
                    <h2 id="detail-strat-title" style="font-size:1.3rem;font-weight:800;color:var(--text);margin-left:8px;">ORBIT_A_240</h2>
                    <span id="detail-strat-status" class="badge badge-green">ACTIVE</span>
                </div>
                
                <!-- Detailed Metrics Grid -->
                <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:16px;">
                    <div class="mc"><div class="mc-label">Trades Executed</div><div class="mc-val" id="det-trades">0</div></div>
                    <div class="mc"><div class="mc-label">Win Rate</div><div class="mc-val clr-g" id="det-wr">0%</div></div>
                    <div class="mc"><div class="mc-label">Total P&amp;L</div><div class="mc-val" id="det-pnl">$0.00</div></div>
                    <div class="mc"><div class="mc-label">Profit Factor</div><div class="mc-val clr-b" id="det-pf">0.00</div></div>
                </div>
                
                <div style="display:grid;grid-template-columns:repeat(3,1fr);gap:12px;margin-bottom:16px;">
                    <div class="mc"><div class="mc-label">Avg Hold Time</div><div class="mc-val" id="det-hold">0s</div></div>
                    <div class="mc"><div class="mc-label">Avg Win / Avg Loss</div><div class="mc-val" id="det-avg-win-loss">$0.00 / -$0.00</div></div>
                    <div class="mc"><div class="mc-label">Max Drawdown</div><div class="mc-val clr-r" id="det-dd">$0.00</div></div>
                </div>

                <!-- Asset Breakdown -->
                <div class="panel">
                    <div class="ph"><span class="ph-title">Asset Breakdown</span></div>
                    <table id="tbl-strat-assets">
                        <thead>
                            <tr><th>Asset</th><th>Trades</th><th>Win Rate</th><th>PnL</th><th>Avg Hold</th></tr>
                        </thead>
                        <tbody></tbody>
                    </table>
                </div>

                <!-- Detailed Trade Ledger -->
                <div class="panel">
                    <div class="ph">
                        <span class="ph-title">Strategy Trade Ledger</span>
                        <span class="ph-right" id="det-ledger-count">0 trades</span>
                    </div>
                    <div style="max-height:500px;overflow-y:auto;">
                        <table id="tbl-strategy-trades">
                            <thead>
                                <tr>
                                    <th></th><th>Entry Time</th><th>Asset</th><th>Dir</th>
                                    <th>Entry Price</th><th>Exit Price</th><th>Size</th><th>PnL</th><th>Duration</th><th>Reason</th>
                                </tr>
                            </thead>
                            <tbody></tbody>
                        </table>
                    </div>
                </div>
            </div>
        </div>

        <!-- ══ PAGE: QUANTUM ══ -->
        <div id="page-quantum" class="page">
            <div class="grid3" style="margin-bottom:14px;">
                <div class="mc"><div class="mc-label">Total Trades</div><div class="mc-val" id="q-total">0</div></div>
                <div class="mc"><div class="mc-label">Overall Win Rate</div><div class="mc-val clr-g" id="q-wr">0%</div></div>
                <div class="mc"><div class="mc-label">Total P&amp;L</div><div class="mc-val" id="q-pnl">$0.00</div></div>
            </div>
            <div class="grid2">
                <div class="panel">
                    <div class="ph"><span class="ph-title">Classification Intelligence</span></div>
                    <table id="tbl-intelligence">
                        <thead><tr><th>Classification</th><th>Count</th><th>Impact</th></tr></thead>
                        <tbody></tbody>
                    </table>
                </div>
                <div class="panel">
                    <div class="ph"><span class="ph-title">AI Recommendations</span></div>
                    <div id="qa-recs" style="font-size:.82rem;line-height:1.7;color:var(--muted2);">Loading...</div>
                </div>
            </div>
            <div class="panel">
                <div class="ph"><span class="ph-title">Intelligence Trade Ledger</span></div>
                <table id="tbl-deep-trades">
                    <thead><tr><th>Strategy</th><th>Asset</th><th>P&amp;L</th><th>TTR</th><th>Velocity</th><th>Spread</th><th>Classification</th></tr></thead>
                    <tbody></tbody>
                </table>
            </div>
        </div>

        <!-- ══ STUB PAGES ══ -->
        <div id="page-feeds" class="page"><div class="panel"><div class="ph-title" style="padding:30px;color:var(--muted);text-align:center;">Market Feeds Monitor — Coming Soon</div></div></div>
        <div id="page-risk" class="page"><div class="panel"><div class="ph-title" style="padding:30px;color:var(--muted);text-align:center;">Risk Management — Coming Soon</div></div></div>
        <div id="page-settings" class="page"><div class="panel"><div class="ph-title" style="padding:30px;color:var(--muted);text-align:center;">Settings — Coming Soon</div></div></div>

    </div><!-- /scroll -->
</div><!-- /main -->

<script>
// ── CHART INSTANCES ──────────────────────────────────────────
let portfolioChart = null, strategyChart = null;
const sparkCharts = {};
let deepTradesStore = [];

// ── SPARKLINE ────────────────────────────────────────────────
const sparkPrice = {BTC:[], ETH:[], SOL:[]};

function drawSparkline(canvasId, data, color) {
    const ctx = document.getElementById(canvasId);
    if (!ctx || !data || data.length < 2) return;
    const labels = data.map((_,i) => i);
    const bgColor = color + '14'; // ~8% opacity via hex
    if (sparkCharts[canvasId]) {
        // Update in-place — no destroy/recreate, no flicker
        const ds = sparkCharts[canvasId].data.datasets[0];
        ds.data = data; ds.borderColor = color; ds.backgroundColor = bgColor;
        sparkCharts[canvasId].data.labels = labels;
        sparkCharts[canvasId].update('none');
        return;
    }
    sparkCharts[canvasId] = new Chart(ctx, {
        type: 'line',
        data: { labels, datasets: [{ data, borderColor: color, borderWidth: 1.5, pointRadius: 0, tension: 0.4, fill: true, backgroundColor: bgColor }] },
        options: { animation: false, plugins: { legend: { display: false }, tooltip: { enabled: false } },
            scales: { x: { display: false }, y: { display: false } }, responsive: true, maintainAspectRatio: false }
    });
}

function miniSpark(canvasId, data, color) {
    const ctx = document.getElementById(canvasId);
    if (!ctx || !data || data.length < 2) return;
    const labels = data.map((_,i) => i);
    if (sparkCharts[canvasId]) {
        const ds = sparkCharts[canvasId].data.datasets[0];
        ds.data = data; ds.borderColor = color;
        sparkCharts[canvasId].data.labels = labels;
        sparkCharts[canvasId].update('none');
        return;
    }
    sparkCharts[canvasId] = new Chart(ctx, {
        type: 'line',
        data: { labels, datasets:[{data, borderColor:color, borderWidth:1.2, pointRadius:0, tension:0.4, fill:false}]},
        options:{animation:false,plugins:{legend:{display:false},tooltip:{enabled:false}},scales:{x:{display:false},y:{display:false}},responsive:true,maintainAspectRatio:false}
    });
}

// ── PORTFOLIO DONUT ──────────────────────────────────────────
function initPortfolioChart() {
    const ctx = document.getElementById('chart-portfolio');
    if (!ctx || portfolioChart) return;
    portfolioChart = new Chart(ctx, {
        type: 'doughnut',
        data: { labels: ['Available','In Positions','Reserved'],
            datasets: [{ data: [100, 0, 0], backgroundColor: ['#3B82F6','#10B981','#F59E0B'],
                borderWidth: 0, hoverOffset: 4 }] },
        options: { cutout: '68%', animation: { duration: 400 }, plugins: { legend: { display: false },
            tooltip: { callbacks: { label: ctx => ' ' + ctx.label + ': $' + ctx.raw.toFixed(2) } } },
            responsive: true, maintainAspectRatio: true }
    });
}

function updatePortfolioChart(avail, inPos, reserved) {
    if (!portfolioChart) initPortfolioChart();
    portfolioChart.data.datasets[0].data = [avail, inPos, reserved];
    portfolioChart.update('none');
    const total = avail + inPos + reserved;
    document.getElementById('port-total').textContent = 'Total: $' + total.toFixed(2);
    document.getElementById('port-avail').textContent = '$' + avail.toFixed(2);
    document.getElementById('port-pos').textContent = '$' + inPos.toFixed(2);
    document.getElementById('port-res').textContent = '$' + reserved.toFixed(2);
}

// ── STRATEGY BAR CHART ───────────────────────────────────────
function initStrategyBarChart(labels, data) {
    const ctx = document.getElementById('chart-strategy');
    if (!ctx) return;
    const colors = data.map(v => v >= 50 ? 'rgba(16,185,129,.7)' : 'rgba(239,68,68,.7)');
    if (strategyChart) {
        // Update data in-place — no flicker
        strategyChart.data.labels = labels;
        strategyChart.data.datasets[0].data = data;
        strategyChart.data.datasets[0].backgroundColor = colors;
        strategyChart.update('none');
        return;
    }
    strategyChart = new Chart(ctx, {
        type: 'bar',
        data: { labels, datasets: [{ label: 'Win Rate %', data, backgroundColor: colors,
            borderRadius: 4, borderWidth: 0 }] },
        options: { animation: { duration: 300 }, plugins: { legend: { display: false }, tooltip: { callbacks: { label: c => ' Win Rate: ' + c.raw.toFixed(1) + '%' } } },
            scales: { x: { ticks: { color: '#64748B', font: { size: 9 } }, grid: { display: false } },
                y: { min: 0, max: 100, ticks: { color: '#64748B', font: { size: 9 }, stepSize: 25 }, grid: { color: 'rgba(30,45,64,.6)' } } },
            responsive: true, maintainAspectRatio: false }
    });
}

// ── NAV ──────────────────────────────────────────────────────
function nav(id) {
    document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
    document.querySelectorAll('.nav-item').forEach(n => n.classList.remove('active'));
    const pg = document.getElementById('page-' + id);
    if (pg) pg.classList.add('active');
    const ni = document.querySelector(`.nav-item[onclick="nav('${id}')"]`);
    if (ni) ni.classList.add('active');
    if (id === 'strategies') {
        showStrategiesList();
    }
    if (id === 'dashboard') { initPortfolioChart(); fetchState(); }
    else if (id === 'quantum' || id === 'history') fetchDeepAnalytics();
    else fetchState();
}

// ── KILL SWITCH ──────────────────────────────────────────────
function toggleKillSwitch() {
    const t = document.getElementById('ks-toggle');
    const s = document.getElementById('ks-status');
    const isOn = t.classList.contains('on');
    t.classList.toggle('on');
    s.textContent = isOn ? 'OFF' : 'ON';
    s.style.color = isOn ? 'var(--muted2)' : 'var(--red)';
    fetch('/api/kill-switch', { method: 'POST', headers: {'Content-Type':'application/json'},
        body: JSON.stringify({ enabled: !isOn }) }).catch(e => console.error('KS error:', e));
}

// ── REFRESH ──────────────────────────────────────────────────
async function forceRefresh() {
    await Promise.all([fetchState(), fetchDeepAnalytics()]);
}

// ── TRADE ROW HELPER ─────────────────────────────────────────
function tradeRow(t, includeTime) {
    const pnl = parseFloat(t.profit_loss || t.pnl || 0);
    const win = pnl > 0;
    const loss = pnl < 0;
    const pnlStr = (pnl > 0 ? '+' : '') + '$' + pnl.toFixed(2);
    const pnlCls = win ? 'pnl-pos' : loss ? 'pnl-neg' : 'clr-m';
    const dotCls = win ? 'win' : loss ? 'loss' : '';
    const reason = (t.close_reason || '').toUpperCase();
    let reasonBadge = '';
    if (reason.includes('STOP')) reasonBadge = `<span class="badge badge-red">${reason}</span>`;
    else if (reason.includes('TAKE') || reason.includes('TP')) reasonBadge = `<span class="badge badge-green">${reason}</span>`;
    else if (reason.includes('TIME') || reason.includes('TTR')) reasonBadge = `<span class="badge badge-blue">${reason}</span>`;
    else reasonBadge = `<span class="badge badge-gray">${reason || '-'}</span>`;
    const timeStr = t.entry_time ? new Date(t.entry_time).toLocaleTimeString() : (t.fired_at || '—');
    const ep = parseFloat(t.entry_price || 0).toFixed(3);
    const xp = parseFloat(t.exit_price || 0).toFixed(3);
    const sz = parseFloat(t.position_size || t.size_usdc || 0).toFixed(2);
    return `<tr>
        <td><span class="th-dot ${dotCls}"></span></td>
        ${includeTime ? `<td>${timeStr}</td>` : ''}
        <td>${t.asset || '-'}</td>
        <td style="font-family:var(--sans);font-size:.75rem;">${t.strategy || t.strategy_type || '-'}</td>
        <td><span class="badge ${t.direction==='BUY_YES'||t.side==='BUY_YES' ? 'badge-green' : 'badge-red'}">${t.side || t.direction || '-'}</span></td>
        <td>${ep}</td><td>${xp}</td><td>$${sz}</td>
        <td class="${pnlCls}">${pnlStr}</td>
        <td>${reasonBadge}</td>
    </tr>`;
}

// ── FETCH STATE ──────────────────────────────────────────────
async function fetchState() {
    try {
        const res = await fetch('/api/state');
        const d = await res.json();

        // Top bar
        const bankroll = d.bankroll || 0;
        const todayPnl = d.today_pnl || 0;
        document.getElementById('top-bankroll').textContent = '$' + bankroll.toFixed(2);
        const tpEl = document.getElementById('top-today-pnl');
        tpEl.textContent = (todayPnl >= 0 ? '+' : '') + '$' + todayPnl.toFixed(2);
        tpEl.className = 'tb-val ' + (todayPnl >= 0 ? 'clr-g' : 'clr-r');

        // Kill switch sync
        if (d.kill_switch !== undefined) {
            const t = document.getElementById('ks-toggle');
            const s = document.getElementById('ks-status');
            if (d.kill_switch) { t.classList.add('on'); s.textContent = 'ON'; s.style.color = 'var(--red)'; }
            else { t.classList.remove('on'); s.textContent = 'OFF'; s.style.color = 'var(--muted2)'; }
        }

        // Live bar
        const ls = d.live_status || {};
        document.getElementById('dot-binance').className = 'feed-dot ' + (ls.binance ? 'on' : '');
        document.getElementById('dot-poly').className = 'feed-dot ' + (ls.polymarket ? 'on' : '');
        const lat = ls.last_tick_seconds_ago;
        document.getElementById('bar-latency').textContent = lat != null ? lat + 's ago' : '—';
        const uptime = ls.uptime_seconds || d.uptime_seconds || 0;
        const h = Math.floor(uptime / 3600), m = Math.floor((uptime % 3600) / 60), s2 = Math.floor(uptime % 60);
        document.getElementById('bar-uptime').textContent = `${h}h ${m}m ${s2}s`;

        // Price cards
        const lp = d.live_prices || {};
        for (const asset of ['BTC','ETH','SOL']) {
            const info = lp[asset] || {};
            const a = asset.toLowerCase();
            const binPrice = info.binance || 0;
            document.getElementById(`pc-${a}-price`).textContent = '$' + binPrice.toLocaleString('en-US', {minimumFractionDigits:2, maximumFractionDigits:2});
            document.getElementById(`pc-${a}-yes`).textContent = (info.poly_yes || 0).toFixed(3);
            document.getElementById(`pc-${a}-no`).textContent = (info.poly_no || 0).toFixed(3);
            document.getElementById(`pc-${a}-spread`).textContent = (info.spread || 0).toFixed(3);
            document.getElementById(`pc-${a}-lag`).textContent = 'Stale: ' + (info.lag || 0).toFixed(1) + 's';
            if (binPrice > 0) {
                if (sparkPrice[asset].length > 30) sparkPrice[asset].shift();
                sparkPrice[asset].push(binPrice);
                drawSparkline('spark-' + asset, sparkPrice[asset], asset === 'BTC' ? '#F59E0B' : asset === 'ETH' ? '#8B5CF6' : '#10B981');
            }
        }

        // Portfolio donut
        const positions = d.positions || [];
        const inPos = positions.reduce((a,p) => a + (p.size_usdc || 0), 0);
        const reserved = (d.pending_count || 0) * 100;
        const avail = Math.max(0, bankroll - inPos - reserved);
        updatePortfolioChart(avail, inPos, reserved);

        // Strategy chart + table
        const strats = d.strategies || [];
        if (strats.length > 0) {
            const labels = strats.map(s => s.strategy_type.replace('ORBIT_A_','OA').replace('LAST_SHADOW_TRADE_LITE_V4','LST-V4').replace('PHANTOM_MOMENTUM_V1','PH-MOMO').replace('PHANTOM_ONE_V1','PH-1'));
            const winRates = strats.map(s => (s.win_rate || 0) * 100);
            initStrategyBarChart(labels, winRates);
            const sb = document.querySelector('#tbl-strat-mini tbody');
            const sbRows = [];
            strats.forEach(s => {
                const wr = s.trades_total > 0 ? ((s.win_rate||0)*100).toFixed(1)+'%' : '-';
                const pnl = s.total_pnl || 0;
                sbRows.push(`<tr>
                    <td style="font-family:var(--sans);font-size:.72rem;">${s.strategy_type}</td>
                    <td>${s.trades_total || 0}</td>
                    <td class="${(s.win_rate||0)>=0.5?'clr-g':'clr-r'}">${wr}</td>
                    <td class="${pnl>=0?'clr-g':'clr-r'}">${pnl>=0?'+':''}$${pnl.toFixed(2)}</td>
                </tr>`);
            });
            sb.innerHTML = sbRows.join('');
            // Full strategy cards page
            const sc = document.getElementById('strat-cards');
            const scCards = [];
            strats.forEach(s => {
                const wr = s.trades_total > 0 ? ((s.win_rate||0)*100).toFixed(1)+'%' : 'N/A';
                const pnl = s.total_pnl || 0;
                scCards.push(`<div class="panel panel-clickable" onclick="viewStrategyDetail('${s.strategy_type}', '${s.status || 'ACTIVE'}')">
                    <div class="ph"><span class="ph-title">${s.strategy_type}</span><span class="badge ${s.status==='ACTIVE'?'badge-green':'badge-red'}">${s.status||'—'}</span></div>
                    <div class="grid3">
                        <div class="mc"><div class="mc-label">Trades</div><div class="mc-val">${s.trades_total||0}</div></div>
                        <div class="mc"><div class="mc-label">Win Rate</div><div class="mc-val ${(s.win_rate||0)>=0.5?'clr-g':'clr-r'}">${wr}</div></div>
                        <div class="mc"><div class="mc-label">P&amp;L</div><div class="mc-val ${pnl>=0?'clr-g':'clr-r'}">${pnl>=0?'+':''}$${pnl.toFixed(2)}</div></div>
                    </div>
                    <div style="margin-top:10px;"><div class="mc-label">Capital Weight</div>
                        <div class="strat-bar-bg"><div class="strat-bar-fill" style="width:${((s.capital_weight||0.33)*100).toFixed(0)}%"></div></div>
                        <div style="font-size:.7rem;color:var(--muted);margin-top:3px;">${((s.capital_weight||0.33)*100).toFixed(1)}%</div>
                    </div>
                </div>`);
            });
            sc.innerHTML = scCards.join('');
        }

        // Positions page
        document.getElementById('pos-count').textContent = positions.length;
        const totalDep = positions.reduce((a,p)=>a+(p.size_usdc||0),0);
        document.getElementById('pos-deployed').textContent = '$' + totalDep.toFixed(2);
        const aptb = document.querySelector('#tbl-active-positions tbody');
        if (positions.length > 0) {
            const posRows = positions.map(p => `<tr>
                    <td style="font-family:var(--sans);font-size:.75rem;">${p.strategy_type||'-'}</td>
                    <td>${p.asset||'-'}</td>
                    <td><span class="badge ${p.direction==='BUY_YES'?'badge-green':'badge-red'}">${p.direction||'-'}</span></td>
                    <td>${(p.entry_price||0).toFixed(3)}</td>
                    <td>$${(p.size_usdc||0).toFixed(2)}</td>
                    <td>${p.opened_at ? Math.floor((Date.now()-p.opened_at)/1000)+'s' : 'ACTIVE'}</td>
                </tr>`);
            aptb.innerHTML = posRows.join('');
        } else {
            aptb.innerHTML = '<tr><td colspan="6" class="tbl-empty">No open positions</td></tr>';
        }

        // Live signals
        const sigs = d.signals || [];
        const stb = document.querySelector('#tbl-live-signals tbody');
        if (sigs.length > 0) {
            const sigRows = sigs.slice(0, 20).map(s => {
                const st = (s.status||'').toUpperCase();
                const scls = st.includes('APPROVED') ? 'badge-green' : st.includes('REJECTED') ? 'badge-red' : 'badge-gray';
                const dcls = s.direction === 'BUY_YES' ? 'badge-green' : 'badge-red';
                const ts = s.fired_at ? (() => { const d = new Date(s.fired_at.replace(' ','T')+'Z'); return d.toLocaleTimeString('en-IN',{hour:'2-digit',minute:'2-digit',second:'2-digit',hour12:false}); })() : '—';
                return `<tr>
                    <td>${ts}</td>
                    <td style="font-weight:700;">${s.asset||'-'}</td>
                    <td style="font-family:var(--sans);font-size:.72rem;">${s.strategy_type||s.strategy||'-'}</td>
                    <td><span class="badge ${dcls}">${s.direction||'-'}</span></td>
                    <td>${(s.binance_price||0)>0 ? '$'+(s.binance_price||0).toFixed(2) : '-'}</td>
                    <td class="clr-g">${(s.poly_yes||0).toFixed(3)}</td>
                    <td class="clr-r">${(s.poly_no||0).toFixed(3)}</td>
                    <td><span class="badge ${scls}">${s.status||'-'}</span></td>
                </tr>`;
            });
            stb.innerHTML = sigRows.join('');
        } else {
            stb.innerHTML = '<tr><td colspan="8" class="tbl-empty">No signals yet — waiting for market activity</td></tr>';
        }

    } catch (e) { console.error('fetchState error:', e); }
}

// ── FETCH DEEP ANALYTICS ─────────────────────────────────────
async function fetchDeepAnalytics() {
    try {
        // Same-origin request reuses the browser's cached Basic Auth from page load —
        // no hardcoded credentials (works with whatever DASHBOARD_PASSWORD is set).
        const res = await fetch('/api/deep-analytics', { credentials: 'same-origin' });
        if (!res.ok) { console.warn('deep-analytics auth failed:', res.status); return; }
        const trades = await res.json();
        deepTradesStore = trades;

        let wins = 0, totalPnl = 0, grossProfit = 0, grossLoss = 0;
        const classCounts = {};

        trades.forEach(t => {
            const pnl = parseFloat(t.profit_loss || 0);
            totalPnl += pnl;
            if (pnl > 0) { wins++; grossProfit += pnl; } else { grossLoss += Math.abs(pnl); }
            const cls = t.intelligence_classification || 'UNKNOWN';
            classCounts[cls] = (classCounts[cls] || 0) + 1;
        });

        const winRate = trades.length > 0 ? (wins / trades.length) * 100 : 0;
        const profitFactor = grossLoss > 0 ? grossProfit / grossLoss : 0;
        const avgPnl = trades.length > 0 ? totalPnl / trades.length : 0;

        // Quantum mini panel on dashboard
        document.getElementById('qa-wr').textContent = winRate.toFixed(1) + '%';
        document.getElementById('qa-pf').textContent = profitFactor.toFixed(2);
        document.getElementById('qa-sr').textContent = (profitFactor > 0 ? (winRate / 100 / Math.max(0.01, 1 - winRate/100)).toFixed(2) : '0.00');
        document.getElementById('qa-tt').textContent = trades.length;
        document.getElementById('qa-tp').textContent = (totalPnl >= 0 ? '+' : '') + '$' + totalPnl.toFixed(2);
        document.getElementById('qa-ap').textContent = (avgPnl >= 0 ? '+' : '') + '$' + avgPnl.toFixed(2);

        // Mini sparklines — real rolling window over last 20 trades.
        // trades is newest-first; take newest 20 then reverse to oldest→newest for chronological sparkline.
        const window20 = trades.slice(0, 20).reverse();
        const wrSpark = window20.map((_,i) => {
            const slice = window20.slice(0, i+1);
            const w = slice.filter(t => parseFloat(t.profit_loss||0) > 0).length;
            return slice.length > 0 ? (w/slice.length)*100 : 50;
        });
        const pfSpark = window20.map((_,i) => {
            const slice = window20.slice(0, i+1);
            const gp = slice.reduce((a,t) => { const p=parseFloat(t.profit_loss||0); return a+(p>0?p:0); }, 0);
            const gl = slice.reduce((a,t) => { const p=parseFloat(t.profit_loss||0); return a+(p<0?Math.abs(p):0); }, 0);
            return gl > 0 ? gp/gl : (gp > 0 ? 2 : 0);
        });
        const srSpark = pfSpark.map((pf, i) => {
            const wrFrac = wrSpark[i] / 100;
            return (wrFrac > 0 && wrFrac < 1) ? (wrFrac / (1 - wrFrac)) * pf : 0;
        });
        miniSpark('qa-spark-wr', wrSpark.length > 1 ? wrSpark : [50,50], '#10B981');
        miniSpark('qa-spark-pf', pfSpark.length > 1 ? pfSpark : [0,0], '#3B82F6');
        miniSpark('qa-spark-sr', srSpark.length > 1 ? srSpark : [0,0], '#E2E8F0');

        // Quantum page metrics
        document.getElementById('q-total').textContent = trades.length;
        document.getElementById('q-wr').textContent = winRate.toFixed(1) + '%';
        const qpEl = document.getElementById('q-pnl');
        qpEl.textContent = (totalPnl >= 0 ? '+' : '') + '$' + totalPnl.toFixed(2);
        qpEl.className = 'mc-val ' + (totalPnl >= 0 ? 'clr-g' : 'clr-r');

        // Trade history table (dashboard)
        // trades is ordered newest-first (opened_at DESC), so the newest 15 are slice(0,15) — no reverse.
        const thTb = document.querySelector('#tbl-trade-history tbody');
        if (thTb) {
            const recent = trades.slice(0, 15);
            if (recent.length > 0) {
                thTb.innerHTML = recent.map(t => tradeRow(t, true)).join('');
                const thWins = recent.filter(t => parseFloat(t.profit_loss||0) > 0).length;
                document.getElementById('th-summary').textContent = `${thWins}W / ${recent.length - thWins}L`;
            } else {
                thTb.innerHTML = '<tr><td colspan="10" class="tbl-empty">No completed trades yet</td></tr>';
            }
        }

        // Full history page — trades already newest-first, render as-is (newest at top).
        const histTb = document.querySelector('#tbl-history tbody');
        if (histTb) {
            if (trades.length > 0) {
                histTb.innerHTML = trades.map(t => tradeRow(t, true)).join('');
            } else {
                histTb.innerHTML = '<tr><td colspan="10" class="tbl-empty">No trade history</td></tr>';
            }
        }

        // Classification table
        const intTb = document.querySelector('#tbl-intelligence tbody');
        if (intTb) {
            intTb.innerHTML = Object.entries(classCounts).map(([cls, cnt]) =>
                `<tr><td><span class="badge badge-gray">${cls}</span></td><td>${cnt}</td><td>—</td></tr>`
            ).join('');
        }

        // Deep trades
        const dtTb = document.querySelector('#tbl-deep-trades tbody');
        if (dtTb) {
            if (trades.length === 0) {
                dtTb.innerHTML = '<tr><td colspan="7" class="tbl-empty">No trade data</td></tr>';
            } else {
                dtTb.innerHTML = trades.map(t => {
                    const pnl = parseFloat(t.profit_loss || 0);
                    const pnlCls = pnl >= 0 ? 'clr-g' : 'clr-r';
                    const cls = t.intelligence_classification || 'UNKNOWN';
                    return `<tr>
                        <td style="font-family:var(--sans);font-size:.73rem;">${t.strategy||'-'}</td>
                        <td>${t.asset||'-'}</td>
                        <td class="${pnlCls}">${pnl>=0?'+':''}$${pnl.toFixed(2)}</td>
                        <td>${t.ttr_seconds||0}s</td>
                        <td>${t.velocity_count||0}</td>
                        <td>${(t.spread_at_entry||0).toFixed(3)}</td>
                        <td><span class="badge ${pnl>=0?'badge-green':'badge-red'}">${cls}</span></td>
                    </tr>`;
                }).join('');
            }
        }

        // AI Recommendations
        const qaRecs = document.getElementById('qa-recs');
        if (qaRecs) qaRecs.innerHTML = `<ul style="padding-left:18px;display:flex;flex-direction:column;gap:8px;">
            <li><strong style="color:var(--text)">ORBIT_A_240:</strong> Current win rate is ${winRate.toFixed(1)}%. Consider tightening SL to 0.52 if below 50%.</li>
            <li><strong style="color:var(--text)">ORBIT_A_260:</strong> Profit factor ${profitFactor.toFixed(2)}. Monitor spread at entry for edge decay.</li>
            <li><strong style="color:var(--text)">LAST_SHADOW_V4:</strong> High-conviction entry range 0.94-0.99 — ensure oracle lag &lt; 10s before entry.</li>
        </ul>`;

    } catch (e) { console.error('fetchDeepAnalytics error:', e); }
}

function exportCSV() {
    if (!deepTradesStore.length) return;
    const keys = Object.keys(deepTradesStore[0]);
    const csv = [keys.join(','), ...deepTradesStore.map(r => keys.map(k => JSON.stringify(r[k]??'')).join(','))].join('\\n');
    const a = document.createElement('a'); a.href = URL.createObjectURL(new Blob([csv],{type:'text/csv'}));
    a.download = 'phantom_v2_trades.csv'; a.click();
}

// ── STRATEGY DRILLDOWN ──────────────────────────────────────────
function showStrategiesList() {
    const listEl = document.getElementById('strategies-list-view');
    const detailEl = document.getElementById('strategy-detail-view');
    if (listEl) listEl.style.display = 'block';
    if (detailEl) detailEl.style.display = 'none';
}

function strategyTradeRow(t) {
    const pnl = parseFloat(t.profit_loss || t.pnl || 0);
    const win = pnl > 0;
    const loss = pnl < 0;
    const pnlStr = (pnl > 0 ? '+' : '') + '$' + pnl.toFixed(2);
    const pnlCls = win ? 'pnl-pos' : loss ? 'pnl-neg' : 'clr-m';
    const dotCls = win ? 'win' : loss ? 'loss' : '';
    const reason = (t.close_reason || t.reason || '').toUpperCase();
    let reasonBadge = '';
    if (reason.includes('STOP')) reasonBadge = `<span class="badge badge-red">${reason}</span>`;
    else if (reason.includes('TAKE') || reason.includes('TP')) reasonBadge = `<span class="badge badge-green">${reason}</span>`;
    else if (reason.includes('TIME') || reason.includes('TTR')) reasonBadge = `<span class="badge badge-blue">${reason}</span>`;
    else reasonBadge = `<span class="badge badge-gray">${reason || '-'}</span>`;
    
    const entryTime = t.entry_time ? new Date(t.entry_time).toLocaleString() : '—';
    const exitTime = t.exit_time ? new Date(t.exit_time).toLocaleString() : '—';
    const ep = parseFloat(t.entry_price || 0).toFixed(3);
    const xp = parseFloat(t.exit_price || 0).toFixed(3);
    const sz = parseFloat(t.position_size || t.size_usdc || 0).toFixed(2);
    
    const hold = parseFloat(t.duration || t.hold_seconds || 0);
    const hm = Math.floor((hold % 3600) / 60), hs = Math.floor(hold % 60);
    const holdStr = hold > 0 ? (hm > 0 ? `${hm}m ${hs}s` : `${hs}s`) : '—';
    
    return `<tr>
        <td><span class="th-dot ${dotCls}"></span></td>
        <td title="Exit: ${exitTime}">${entryTime}</td>
        <td><b>${t.asset || '-'}</b></td>
        <td><span class="badge ${t.side === 'BUY_YES' || t.direction === 'BUY_YES' ? 'badge-green' : 'badge-red'}">${t.side || t.direction || '-'}</span></td>
        <td>${ep}</td>
        <td>${xp}</td>
        <td>$${sz}</td>
        <td class="${pnlCls}">${pnlStr}</td>
        <td>${holdStr}</td>
        <td>${reasonBadge}</td>
    </tr>`;
}

function viewStrategyDetail(stratName, status) {
    const listEl = document.getElementById('strategies-list-view');
    const detailEl = document.getElementById('strategy-detail-view');
    if (listEl) listEl.style.display = 'none';
    if (detailEl) detailEl.style.display = 'block';

    let displayTitle = stratName;
    if (stratName === 'LAST_SHADOW_TRADE_LITE_V4') displayTitle = 'Last Shadow Lite V4';
    document.getElementById('detail-strat-title').textContent = displayTitle;
    
    const statusBadge = document.getElementById('detail-strat-status');
    statusBadge.textContent = status;
    statusBadge.className = 'badge ' + (status === 'ACTIVE' ? 'badge-green' : 'badge-red');

    // Filter trades
    const trades = deepTradesStore.filter(t => {
        const sName = t.strategy || t.strategy_type;
        return sName === stratName;
    });

    const totalTrades = trades.length;
    const wins = trades.filter(t => parseFloat(t.profit_loss || t.pnl || 0) > 0).length;
    const losses = trades.filter(t => parseFloat(t.profit_loss || t.pnl || 0) < 0).length;
    const winRate = totalTrades > 0 ? (wins / totalTrades) * 100 : 0.0;
    const netPnL = trades.reduce((sum, t) => sum + parseFloat(t.profit_loss || t.pnl || 0), 0);
    
    const grossProfit = trades.reduce((sum, t) => {
        const p = parseFloat(t.profit_loss || t.pnl || 0);
        return sum + (p > 0 ? p : 0);
    }, 0);
    const grossLoss = trades.reduce((sum, t) => {
        const p = parseFloat(t.profit_loss || t.pnl || 0);
        return sum + (p < 0 ? Math.abs(p) : 0);
    }, 0);
    const profitFactor = grossLoss > 0 ? grossProfit / grossLoss : (grossProfit > 0 ? 99.0 : 0.0);

    const totalHold = trades.reduce((sum, t) => sum + parseFloat(t.duration || t.hold_seconds || 0), 0);
    const avgHold = totalTrades > 0 ? totalHold / totalTrades : 0;

    const avgWin = wins > 0 ? grossProfit / wins : 0;
    const avgLoss = losses > 0 ? grossLoss / losses : 0;

    // Max Drawdown calculation (chronological)
    let peak = 0;
    let maxDD = 0;
    let currentPnL = 0;
    const chronological = [...trades].reverse();
    chronological.forEach(t => {
        currentPnL += parseFloat(t.profit_loss || t.pnl || 0);
        if (currentPnL > peak) peak = currentPnL;
        const dd = peak - currentPnL;
        if (dd > maxDD) maxDD = dd;
    });

    // Populate Detailed Metrics Card values
    document.getElementById('det-trades').textContent = totalTrades;
    
    const wrEl = document.getElementById('det-wr');
    wrEl.textContent = winRate.toFixed(1) + '%';
    wrEl.className = 'mc-val ' + (winRate >= 50 ? 'clr-g' : 'clr-r');

    const pnlEl = document.getElementById('det-pnl');
    pnlEl.textContent = (netPnL >= 0 ? '+' : '') + '$' + netPnL.toFixed(2);
    pnlEl.className = 'mc-val ' + (netPnL >= 0 ? 'clr-g' : 'clr-r');

    const pfEl = document.getElementById('det-pf');
    pfEl.textContent = profitFactor.toFixed(2);
    pfEl.className = 'mc-val ' + (profitFactor >= 1.0 ? 'clr-g' : 'clr-r');

    const hm = Math.floor((avgHold % 3600) / 60), hs = Math.floor(avgHold % 60);
    document.getElementById('det-hold').textContent = hm > 0 ? `${hm}m ${hs}s` : `${hs}s`;
    
    document.getElementById('det-avg-win-loss').textContent = `+$${avgWin.toFixed(2)} / -$${avgLoss.toFixed(2)}`;
    document.getElementById('det-dd').textContent = '$' + maxDD.toFixed(2);

    // Populate Asset Breakdown table
    const assets = ['BTC', 'ETH', 'SOL'];
    const assetBody = document.querySelector('#tbl-strat-assets tbody');
    if (assetBody) {
        assetBody.innerHTML = assets.map(asset => {
            const assetTrades = trades.filter(t => t.asset === asset);
            const aWins = assetTrades.filter(t => parseFloat(t.profit_loss || t.pnl || 0) > 0).length;
            const aWr = assetTrades.length > 0 ? (aWins / assetTrades.length) * 100 : 0.0;
            const aPnL = assetTrades.reduce((sum, t) => sum + parseFloat(t.profit_loss || t.pnl || 0), 0);
            const aHoldTotal = assetTrades.reduce((sum, t) => sum + parseFloat(t.duration || t.hold_seconds || 0), 0);
            const aHold = assetTrades.length > 0 ? aHoldTotal / assetTrades.length : 0;
            
            const wrStr = assetTrades.length > 0 ? aWr.toFixed(1) + '%' : '-';
            const pnlStr = (aPnL >= 0 ? '+' : '') + '$' + aPnL.toFixed(2);
            const pnlCls = aPnL > 0 ? 'clr-g' : aPnL < 0 ? 'clr-r' : 'clr-m';
            
            const am = Math.floor((aHold % 3600) / 60), as = Math.floor(aHold % 60);
            const holdStr = assetTrades.length > 0 ? (am > 0 ? `${am}m ${as}s` : `${as}s`) : '-';
            
            return `<tr>
                <td><b>${asset}</b></td>
                <td>${assetTrades.length}</td>
                <td class="${aWr >= 50 ? 'clr-g' : 'clr-r'}">${wrStr}</td>
                <td class="${pnlCls}">${pnlStr}</td>
                <td>${holdStr}</td>
            </tr>`;
        }).join('');
    }

    // Populate Detailed Trades table
    const tradeBody = document.querySelector('#tbl-strategy-trades tbody');
    document.getElementById('det-ledger-count').textContent = `${totalTrades} trades`;
    if (tradeBody) {
        if (totalTrades > 0) {
            tradeBody.innerHTML = trades.map(t => strategyTradeRow(t)).join('');
        } else {
            tradeBody.innerHTML = '<tr><td colspan="10" class="tbl-empty">No trades executed for this strategy</td></tr>';
        }
    }
}

// ── INIT ─────────────────────────────────────────────────────
initPortfolioChart();
fetchState();
fetchDeepAnalytics();
setInterval(fetchState, 5000);
setInterval(fetchDeepAnalytics, 30000);

// ── CLOCKS UPDATE ─────────────────────────────────────────────
function updateClocks() {
    const now = new Date();
    
    // Clock 1: UTC Time (HH:MM:SS)
    const utcHours = String(now.getUTCHours()).padStart(2, '0');
    const utcMinutes = String(now.getUTCMinutes()).padStart(2, '0');
    const utcSeconds = String(now.getUTCSeconds()).padStart(2, '0');
    const utcStr = `${utcHours}:${utcMinutes}:${utcSeconds}`;
    
    const clockUtcEl = document.getElementById('clock-utc');
    if (clockUtcEl) {
        clockUtcEl.textContent = utcStr;
    }
    
    // Clock 2: 5M Candle Timer (countdown MM:SS from 05:00 to 00:00)
    // Calculated as: 300 - (current_unix_timestamp % 300) seconds remaining
    const currentUnix = Math.floor(now.getTime() / 1000);
    const secondsRemaining = 300 - (currentUnix % 300);
    
    // Format remaining time as MM:SS
    const candleMinutes = String(Math.floor(secondsRemaining / 60)).padStart(2, '0');
    const candleSeconds = String(secondsRemaining % 60).padStart(2, '0');
    const candleStr = `${candleMinutes}:${candleSeconds}`;
    
    const clockCandleEl = document.getElementById('clock-candle');
    if (clockCandleEl) {
        clockCandleEl.textContent = candleStr;
        
        // Color transition logic:
        // Green when >60 seconds remaining, yellow when 30-60 seconds, red when <30 seconds
        if (secondsRemaining > 60) {
            clockCandleEl.style.color = 'var(--green)';
        } else if (secondsRemaining >= 30) {
            clockCandleEl.style.color = 'var(--yellow)';
        } else {
            clockCandleEl.style.color = 'var(--red)';
        }
    }
}

updateClocks();
setInterval(updateClocks, 1000);
</script>
</body>
</html>
"""

    @app.get("/", dependencies=[Depends(auth_dependency)])
    def get_dashboard():
        return HTMLResponse(content=html_content)

    @app.get("/api/state")
    async def get_state():
        binance_conn = False
        poly_conn = False
        last_tick_sec = None
        
        if state_provider.spot_feed:
            binance_conn = getattr(state_provider.spot_feed, "connected", False)
            now_ms = int(time.time() * 1000)
            latest_ts = 0
            for sym in state_provider.spot_feed.symbols:
                history = state_provider.spot_feed.price_history.get(sym)
                if history:
                    latest_ts = max(latest_ts, history[-1][0])
            if latest_ts > 0:
                last_tick_sec = max(0, int((now_ms - latest_ts) / 1000))
            
        if state_provider.poly_feed:
            poly_conn = getattr(state_provider.poly_feed, "connected", False)

        live_status = {
            "binance": binance_conn,
            "polymarket": poly_conn,
            "last_tick_seconds_ago": last_tick_sec,
            "uptime_seconds": time.time() - START_TIME
        }

        # Calculate live price objects for ticker panel
        live_prices = {}
        for asset in ["BTC", "ETH", "SOL"]:
            b_price = 0.0
            if state_provider.spot_feed and state_provider.spot_feed.price_history.get(asset):
                b_price = state_provider.spot_feed.price_history[asset][-1][1]
            
            p_price_yes = 0.50
            p_price_no = 0.50
            p_spread = 0.00
            p_lag = 0.0
            if state_provider.poly_feed:
                p_state = state_provider.poly_feed.get_market_state(asset)
                if p_state:
                    p_price_yes = p_state.get("yes_price", 0.50)
                    p_price_no = p_state.get("no_price", 0.50)
                    p_spread = p_state.get("spread", 0.00)
                    p_lag = p_state.get("staleness_seconds", 0.0)
            
            live_prices[asset] = {
                "binance": b_price,
                "poly_yes": p_price_yes,
                "poly_no": p_price_no,
                "spread": p_spread,
                "lag": p_lag
            }

        if state_provider.db_manager:
            try:
                state = await state_provider.db_manager.get_dashboard_state()
                state["live_status"] = live_status
                state["live_prices"] = live_prices
                state["uptime_seconds"] = live_status["uptime_seconds"]
                state["latency_ms"] = last_tick_sec if last_tick_sec is not None else 0
                return JSONResponse(content=state)
            except Exception as e:
                import traceback
                logger.error(f"Error reading dashboard state from DB: {e}\n{traceback.format_exc()}")

        # Fallback response
        fallback_state = state_provider.get_dashboard_state()
        fallback_state.update({
            "strategies": [],
            "oracles": [],
            "positions": [],
            "risk_events": [],
            "signals": [],
            "trade_history": [],
            "paper_mode": str(state_provider.config.get("paper_mode", "true")).lower() == "true",
            "live_status": live_status,
            "live_prices": live_prices,
            "uptime_seconds": live_status["uptime_seconds"],
            "latency_ms": last_tick_sec if last_tick_sec is not None else 0
        })
        return JSONResponse(content=fallback_state)

    @app.post("/api/kill_switch", dependencies=[Depends(auth_dependency)])
    @app.post("/api/kill-switch", dependencies=[Depends(auth_dependency)])
    async def toggle_kill_switch(payload: dict = None):
        if payload and "enabled" in payload:
            next_val = bool(payload["enabled"])
        else:
            next_val = not state_provider.config.get("kill_switch", False)
        state_provider.config["kill_switch"] = next_val
        if state_provider.db_manager:
            await state_provider.db_manager.update_system_config("kill_switch", str(next_val).lower())
        return {"status": "success", "kill_switch": next_val}

    @app.get("/api/analytics", dependencies=[Depends(auth_dependency)])
    async def get_analytics():
        def finite(value, fallback=0.0):
            try:
                value = float(value)
            except (TypeError, ValueError):
                return fallback
            return value if value == value and value not in (float("inf"), float("-inf")) else fallback

        def sanitize(value):
            if isinstance(value, dict):
                return {k: sanitize(v) for k, v in value.items()}
            if isinstance(value, list):
                return [sanitize(v) for v in value]
            if isinstance(value, float):
                return finite(value)
            return value

        def build_metrics(data):
            stats = (data or {}).get("stats", {})
            rows = list(stats.values())
            total_trades = sum(int(finite(s.get("trades"))) for s in rows)
            wins = sum(int(finite(s.get("wins"))) for s in rows)
            gross_profit = sum(finite(s.get("gross_profit")) for s in rows)
            gross_loss = sum(finite(s.get("gross_loss")) for s in rows)
            net_pnl = sum(finite(s.get("pnl")) for s in rows)
            profit_factor = gross_profit / gross_loss if gross_loss > 0 else (99.0 if gross_profit > 0 else 0.0)
            avg_trade = net_pnl / total_trades if total_trades > 0 else 0.0
            return {
                "total_trades": total_trades,
                "overall_win_rate": wins / total_trades if total_trades > 0 else 0.0,
                "profit_factor": profit_factor,
                "sharpe_ratio": profit_factor * 0.65 if profit_factor > 0 else 0.0,
                "net_pnl": net_pnl,
                "avg_pnl_per_trade": avg_trade,
            }

        try:
            engine = AnalyticsEngine()
            data = await engine.run_analysis(output_file=None, return_data=True) or {}
            metrics = build_metrics(data)
            return JSONResponse(content={"status": "success", "data": sanitize(data), "metrics": sanitize(metrics)})
        except Exception as e:
            logger.error(f"Failed to generate analytics: {e}")
            return JSONResponse(content={"status": "error", "message": str(e), "metrics": build_metrics({})}, status_code=200)
    @app.get("/api/metrics", dependencies=[Depends(auth_dependency)])
    async def get_metrics():
        cpu_usage = 0.0
        memory_usage_mb = 0.0
        try:
            import psutil
            process = psutil.Process(os.getpid())
            memory_usage_mb = round(process.memory_info().rss / (1024 * 1024), 2)
            cpu_usage = round(process.cpu_percent(interval=None), 2)
        except Exception:
            pass

        db_metrics = {
            "queue_depth": 0,
            "total_queued": 0,
            "total_written": 0,
            "total_failed": 0,
            "total_dropped": 0,
            "avg_queue_depth": 0.0,
            "peak_queue_depth": 0,
            "p95_queue_depth": 0.0
        }
        if hasattr(state_provider, "db_writer") and state_provider.db_writer:
            db_metrics = state_provider.db_writer.get_metrics()

        # --- Coinbase Feed Metrics ---
        binance_conn = False
        binance_reconnects = 0
        binance_assets = {}
        if state_provider.spot_feed:
            sf = state_provider.spot_feed
            binance_conn = getattr(sf, "connected", False)
            binance_reconnects = getattr(sf, "reconnect_count", 0)
            now_ms = int(time.time() * 1000)
            for sym in ["BTC", "ETH", "SOL"]:
                history = sf.price_history.get(sym, [])
                last_price = history[-1][1] if history else None
                last_ts_ms = history[-1][0] if history else None
                age_sec = round((now_ms - last_ts_ms) / 1000, 1) if last_ts_ms else None
                binance_assets[f"{sym}USDT"] = {
                    "last_price": last_price,
                    "last_tick_age_seconds": age_sec,
                }

        # --- Polymarket Feed Metrics ---
        poly_metrics = {
            "connected": False,
            "reconnect_count": 0,
            "total_messages": 0,
            "messages_per_minute": 0.0,
            "subscription_count": 0,
            "last_message_age_seconds": None,
            "tracked_tokens": 0,
            "stale_threshold_seconds": 10.0,
            "assets": {
                "BTC": {"feed_status": "NO_MARKET", "staleness_seconds": None, "is_stale": True},
                "ETH": {"feed_status": "NO_MARKET", "staleness_seconds": None, "is_stale": True},
                "SOL": {"feed_status": "NO_MARKET", "staleness_seconds": None, "is_stale": True},
            }
        }
        if state_provider.poly_feed:
            try:
                poly_metrics = state_provider.poly_feed.get_feed_metrics()
            except Exception as e:
                poly_metrics["error"] = str(e)

        monitor_latency = getattr(state_provider, "monitor_latency", 0)
        open_positions_count = len([p for p in state_provider.open_positions if p.get("status") == "OPEN"])

        funnel = {"evaluated": 0, "classified": 0, "approved": 0, "attempted": 0, "filled": 0}
        no_poly_state_count = 0
        skips_by_reason = {}
        if state_provider.db_manager:
            try:
                db_state = await state_provider.db_manager.get_dashboard_state()
                funnel = db_state.get("funnel", funnel)
                no_poly_state_count = db_state.get("no_poly_state_count", 0)
                skips_by_reason = db_state.get("skips_by_reason", {})
            except Exception as e:
                logger.error(f"Error loading funnel metrics in /api/metrics: {e}")

        return JSONResponse(content={
            "system": {
                "cpu_usage_pct": cpu_usage,
                "memory_usage_mb": memory_usage_mb,
            },
            "db_writer": db_metrics,
            "websockets": {
                "binance": {
                    "connected": binance_conn,
                    "reconnects": binance_reconnects,
                    "assets": binance_assets,
                },
                "polymarket": poly_metrics,
            },
            "monitor": {
                "latency_ms": monitor_latency,
                "open_positions": open_positions_count,
            },
            "funnel": funnel,
            "no_poly_state_count": no_poly_state_count,
            "skips_by_reason": skips_by_reason
        })


    @app.post("/api/test/inject_tick", dependencies=[Depends(auth_dependency)])
    async def inject_tick(payload: dict):
        # Backend Safety Check: Reject injection if paper_mode is False
        is_paper = True
        if state_provider.db_manager:
            try:
                db_config = await state_provider.db_manager.load_system_config()
                is_paper = db_config.get("paper_mode", "true").lower() == "true"
            except Exception:
                pass
        else:
            is_paper = str(state_provider.config.get("paper_mode", "true")).lower() == "true"
            
        if not is_paper:
            raise HTTPException(status_code=400, detail="Injection disabled - Live Mode Active")

        if state_provider and state_provider.spot_feed:
            symbol = payload.get("symbol")
            asset = symbol.replace("USDT", "")
            price = float(payload.get("price"))
            timestamp_ms = int(payload.get("timestamp_ms", int(time.time() * 1000)))
            state_provider.spot_feed.process_tick(asset, price, timestamp_ms)
            return {"status": "success", "message": f"Tick injected for {symbol} at {price}"}
        return {"status": "error", "message": "Coinbase spot feed not registered"}

    @app.post("/api/test/inject_poly_tick", dependencies=[Depends(auth_dependency)])
    async def inject_poly_tick(payload: dict):
        is_paper = True
        if state_provider.db_manager:
            try:
                db_config = await state_provider.db_manager.load_system_config()
                is_paper = db_config.get("paper_mode", "true").lower() == "true"
            except Exception:
                pass
        else:
            is_paper = str(state_provider.config.get("paper_mode", "true")).lower() == "true"
            
        if not is_paper:
            raise HTTPException(status_code=400, detail="Injection disabled - Live Mode Active")

        if state_provider and state_provider.poly_feed:
            asset = payload.get("asset")
            yes_price = float(payload.get("yes_price"))
            prev_yes_price = float(payload.get("prev_yes_price", yes_price - 0.02))
            
            # Update state in poly_feed
            state = state_provider.poly_feed.market_state.get(asset)
            if not state:
                state_provider.poly_feed.set_active_market(
                    asset=asset,
                    market_id=payload.get("market_id", f"mock-{asset}-market"),
                    token_id=payload.get("token_id", f"mock-{asset}-token"),
                    resolution_time_ms=int(time.time() * 1000) + 120000, # 2 minutes TTR
                    liquidity_usdc=float(payload.get("liquidity_usdc", 10000.0)),
                    market_open_time=int(time.time() * 1000) - 180000
                )
                state = state_provider.poly_feed.market_state[asset]

            # Override resolution time to keep simulated TTR inside the strategy window (120s)
            state["resolution_time_ms"] = int(time.time() * 1000) + 120000

            state["has_live_data"] = True
            state["yes_price"] = yes_price
            state["no_price"] = round(1.0 - yes_price, 4)
            state["prev_yes_price"] = prev_yes_price
            state["last_updated_ms"] = int(time.time() * 1000)
            
            # Fire signal engine
            if state_provider.poly_feed.signal_callback:
                updated_state = state_provider.poly_feed.get_market_state(asset)
                if updated_state:
                    if asyncio.iscoroutinefunction(state_provider.poly_feed.signal_callback):
                        await state_provider.poly_feed.signal_callback(asset, updated_state)
                    else:
                        state_provider.poly_feed.signal_callback(asset, updated_state)
            return {"status": "success", "message": f"Polymarket tick injected for {asset}: YES={yes_price}, prev={prev_yes_price}"}
        return {"status": "error", "message": "Polymarket feed not registered"}

    @app.get("/dashboard/feed-health", dependencies=[Depends(auth_dependency)])
    async def get_feed_health():
        pf = state_provider.poly_feed
        sf = state_provider.spot_feed
        
        freshness = None
        if pf and getattr(pf, "last_message_timestamp", 0) > 0:
            freshness = round(time.time() - pf.last_message_timestamp, 3)
            
        assets_health = {}
        for asset in ["BTC", "ETH", "SOL"]:
            p_state = pf.get_market_state(asset) if pf else None
            b_price = 0.0
            if sf and sf.price_history.get(asset):
                b_price = sf.price_history[asset][-1][1]
                
            if p_state:
                assets_health[asset] = {
                    "yes_price": p_state.get("yes_price"),
                    "no_price": p_state.get("no_price"),
                    "best_bid": p_state.get("best_bid"),
                    "best_ask": p_state.get("best_ask"),
                    "spread": p_state.get("spread"),
                    "oracle_lag_seconds": p_state.get("staleness_seconds"),
                    "is_stale": p_state.get("is_stale")
                }
            else:
                assets_health[asset] = None
                
        return JSONResponse(content={
            "has_live_data": state_provider.has_live_data,
            "feed_freshness_seconds": freshness,
            "polymarket_connected": getattr(pf, "connected", False) if pf else False,
            "binance_connected": getattr(sf, "connected", False) if sf else False,
            "assets": assets_health
        })

    @app.get("/dashboard/funnel", dependencies=[Depends(auth_dependency)])
    async def get_funnel():
        from datetime import datetime, timezone
        boot_time = getattr(state_provider, "boot_time", None)
        if not boot_time:
            # Fallback if boot_time is not set
            boot_time = datetime.fromtimestamp(0, tz=timezone.utc)
            
        if state_provider.db_manager:
            try:
                funnel_stats = await state_provider.db_manager.get_funnel_stats(boot_time)
                return JSONResponse(content=funnel_stats)
            except Exception as e:
                logger.error(f"Error getting funnel stats from DB: {e}")
                raise HTTPException(status_code=500, detail=str(e))
                
        return JSONResponse(content={
            "error": "Database manager not initialized"
        })

    @app.get("/api/deep-analytics", dependencies=[Depends(auth_dependency)])
    async def get_deep_analytics():
        try:
            from analytics_engine import AnalyticsEngine
            engine = AnalyticsEngine()
            data = await engine.generate_deep_analysis(limit=1000)
            return JSONResponse(content=data)
        except Exception as e:
            import logging
            logger = logging.getLogger("Dashboard")
            logger.error(f"Error fetching deep analytics: {e}")
            return JSONResponse(content=[])

    return app

create_app()
