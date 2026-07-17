import asyncio
import logging
import time
import os
import urllib.request
import json
from datetime import datetime, timezone
import uvicorn
from dotenv import load_dotenv

from poly_feed import PolyFeed
from signal_engine import SignalEngine
from risk_engine import RiskEngine
from signal_validator import SignalValidator
from executor import Executor
from monitor import PositionMonitor
from strategy_stats import StrategyStatsEngine
from strategies import (
    last_shadow_driver,
    last_shadow_settlement_sweep,
    phantom_one_driver,
    phantom_one_settlement_sweep,
)
from spot_feed import SpotFeed
from telegram import TelegramBot
from dashboard import app, state_provider
from database import DatabaseManager
from database_writer import DatabaseWriter

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def fire_and_forget(coro):
    task = asyncio.create_task(coro)
    def callback(t):
        try:
            t.result()
        except Exception as e:
            logger.error(f"Background task failed: {e}", exc_info=True)
    task.add_done_callback(callback)
    return task

# Global database manager reference
db = None
db_writer = None
tg = None
eval_count = 0
boot_time = datetime.now(timezone.utc)

async def trigger_100_eval_report(count):
    if db is None or tg is None:
        return
    try:
        stats = await db.get_100_eval_report_stats(boot_time)
        
        # Format report
        rejections_str = ""
        if stats["rejections"]:
            rejections_str = "\n".join([f"  • {reason}: {c}" for reason, c in stats["rejections"].items()])
        else:
            rejections_str = "  • None"
            
        win_rate = 0.0
        total_closed = stats["wins"] + stats["losses"]
        if total_closed > 0:
            win_rate = (stats["wins"] / total_closed) * 100
            
        report = (
            f"📊 <b>PHANTOM 100-EVAL REPORT (Count: {count})</b>\n"
            f"• Total Evaluations (since boot): {stats['total_evals']}\n"
            f"• Approved/Executed: {stats['approved']}\n"
            f"• Rejected by Risk: {stats['rejected']}\n"
            f"• Skipped (Strategy): {stats['skipped']}\n"
            f"• Superseded: {stats['superseded']}\n\n"
            f"🚫 <b>Risk Rejections Breakdown:</b>\n"
            f"{rejections_str}\n\n"
            f"📈 <b>Execution Stats (since boot):</b>\n"
            f"• Trades Executed: {stats['trades_executed']}\n"
            f"• Closed Wins: {stats['wins']}\n"
            f"• Closed Losses: {stats['losses']}\n"
            f"• Win Rate: {win_rate:.1f}%\n"
            f"• Net P&L: {stats['net_pnl']:+.2f} USDC"
        )
        
        logger.info(f"\n[REPORT] {report}\n")
        await tg.send_alert(report)
    except Exception as e:
        logger.error(f"Error generating 100-eval report: {e}", exc_info=True)

async def real_db_callback(arg1, arg2=None):
    global eval_count
    if db_writer is None:
        logger.debug(f"DB writer not initialized, skipping log for {arg1}")
        return
        
    try:
        is_signal = False
        if arg2 is None:
            is_signal = True
            await db_writer.write("signals", arg1)
        else:
            if arg1 == "signals":
                is_signal = True
            await db_writer.write(arg1, arg2)
            
        if is_signal:
            eval_count += 1
            if eval_count % 100 == 0:
                fire_and_forget(trigger_100_eval_report(eval_count))
    except Exception as e:
        logger.error(f"Error in real_db_callback for {arg1}: {e}")

def get_60s_delta(price_history, current_price, now_ms):
    target_ms = now_ms - 60000
    closest_price = None
    closest_diff = float('inf')
    for ts, price in price_history:
        diff = abs(ts - target_ms)
        if diff < closest_diff:
            closest_diff = diff
            closest_price = price
    if closest_price is not None and closest_price > 0:
        return (current_price - closest_price) / closest_price * 100
    return 0.0

async def main():
    global db
    load_dotenv()
    logger.info("Initializing Phantom V2...")
    
    # PHASE 0 TOGGLE
    # Global phase switch
    PHASE_0_MODE = False  # Disabled to allow normal strategies to trade.
    logger.info(f"PHASE_0_MODE is set to: {PHASE_0_MODE}")

    # Initialize Telegram bot first (needed as callback for DB writer)
    global tg
    tg = TelegramBot()
    state_provider.boot_time = boot_time
    state_provider.telegram_callback = tg.send_alert

    # 1. Initialize Database
    global db
    database_url = os.getenv("DATABASE_URL")
    if database_url:
        try:
            db = DatabaseManager(database_url)
            await db.initialize()
            # Run migrations automatically on start
            await db.run_migrations("schema.sql")
            state_provider.db_manager = db
            
            # Start database write queue
            global db_writer
            db_writer = DatabaseWriter(db, telegram_callback=tg.send_alert)
            db_writer.start()
            state_provider.db_writer = db_writer
            
            # Load open positions from DB to resume monitoring
            try:
                state_provider.open_positions = await db.load_open_positions()
                logger.info(f"Loaded {len(state_provider.open_positions)} open positions from database.")
            except Exception as e:
                logger.error(f"Failed to load open positions from database: {e}")
                
            # Load historical asset lag stats from DB
            try:
                state_provider.lag_stats = await db.load_asset_lag_stats()
                logger.info(f"Loaded historical asset lag stats from database: {state_provider.lag_stats}")
            except Exception as e:
                logger.error(f"Failed to load asset lag stats from database: {e}")
                
            # Load daily stats and consecutive losses from DB
            try:
                from datetime import date
                daily_stats = await db.load_daily_stats_and_losses()
                state_provider._today_trades = daily_stats["today_trades"]
                state_provider._today_wins = daily_stats["today_wins"]
                state_provider._today_losses = daily_stats["today_losses"]
                state_provider._today_pnl = daily_stats["today_pnl"]
                state_provider._today_date = date.today()
                state_provider._consecutive_losses = daily_stats["consecutive_losses"]
                state_provider._last_loss_time = daily_stats["last_loss_time"]
                logger.info(f"Loaded daily stats from DB: trades={state_provider._today_trades}, wins={state_provider._today_wins}, losses={state_provider._today_losses}, pnl={state_provider._today_pnl}, consecutive_losses={state_provider._consecutive_losses}")
            except Exception as e:
                logger.error(f"Failed to load daily stats from database: {e}")

            # Restore bankroll from cumulative closed-position PnL so daily loss limits
            # are enforced against real equity, not the hardcoded default of $1000.
            try:
                async with db.pool.acquire() as conn:
                    row = await conn.fetchrow(
                        "SELECT COALESCE(SUM(pnl), 0.0) AS cumulative_pnl FROM positions WHERE status IN ('CLOSED', 'STOPPED')"
                    )
                cumulative_pnl = float(row["cumulative_pnl"]) if row else 0.0
                state_provider.bankroll = round(1000.0 + cumulative_pnl, 4)
                logger.info(f"Bankroll restored from DB: $1000.00 base + ${cumulative_pnl:+.2f} PnL = ${state_provider.bankroll:.2f}")
            except Exception as e:
                logger.error(f"Failed to restore bankroll from database: {e}")
            logger.info("Database initialized, write queue started, and migrations applied successfully.")
        except Exception as e:
            logger.error(f"Failed to initialize database: {e}. Bot will run with in-memory fallback.")
            db = None
            db_writer = None
    else:
        logger.warning("DATABASE_URL not set. Running with in-memory mock fallback.")

    # The dashboard has the shared state_provider instance
    sp = state_provider
    
    # 2. Stats Engine
    stats_engine = StrategyStatsEngine(telegram_callback=tg.send_alert, db_callback=real_db_callback)
    sp.strategy_engine = stats_engine
    sp.phase_0_mode = PHASE_0_MODE

    # 3. Polymarket Feed
    poly_feed = PolyFeed()
    sp.poly_feed = poly_feed

    # 4. Engines
    signal_engine = SignalEngine(poly_feed=poly_feed, db_callback=real_db_callback)
    poly_feed.signal_callback = signal_engine.process_poly_signal
    risk_engine = RiskEngine(state_provider=sp, db_callback=real_db_callback)
    
    # 4b. Signal Validator
    signal_validator = SignalValidator(state_provider=sp)
    signal_validator.start()
    sp.signal_validator = signal_validator
    
    # ── Startup sync guard: strategy classifier TTR ceilings must match risk engine ──
    # This catches the class of bug where orbit_a_240.py says 240 but risk_engine says 260.
    # ORBIT strategies are retired (removed from signal_engine) — only validate live ones.
    _STRATEGY_TTR_CEILINGS = {
        "LAST_SHADOW_TRADE_LITE_V4": 15,
        "ORBIT_A_240": 210,
        "PHANTOM_MOMENTUM_V1": 200,
    }
    for _strat, _expected_max in _STRATEGY_TTR_CEILINGS.items():
        _re_window = risk_engine.VALID_TTR_WINDOWS.get(_strat)
        if _re_window and _re_window[1] != _expected_max:
            logger.critical(
                f"TTR SYNC ERROR: {_strat} classifier max={_expected_max}s "
                f"but risk_engine window max={_re_window[1]}s — fix before trading!"
            )
            raise SystemExit(1)
    logger.info("TTR sync validation passed — all strategy windows are consistent.")

    # 5. Executor
    executor = Executor(risk_engine, db_callback=real_db_callback, telegram_callback=tg.send_alert, state_provider=sp)
    await executor.initialize()
    
    # Boot recovery of active DCA journals
    if db is not None:
        try:
            active_journals = await db.load_active_dca_journals()
            if active_journals:
                logger.info(f"Discovered {len(active_journals)} active DCA journals for recovery.")
                for journal in active_journals:
                    signal_id = journal["signal_id"]
                    signal = await db.load_signal_by_id(signal_id)
                    if signal:
                        await executor.resume_dca(signal, journal)
                    else:
                        logger.error(f"Failed to load signal {signal_id} for recovery of journal {journal['id']}.")
            else:
                logger.info("No active DCA journals found for recovery.")
        except Exception as e:
            logger.error(f"Error during DCA recovery boot sequence: {e}", exc_info=True)
    
    # Wire signal -> risk -> executor
    async def handle_signal(signal):
        asset = signal["asset"]

        # Synchronous pre-flight: drop obvious duplicates before any await.
        if asset in state_provider.pending_assets or any(
            p.get("asset") == asset and p.get("status") == "OPEN"
            for p in state_provider.open_positions
        ):
            logger.debug(f"[SIGNAL] {asset} already active — dropping duplicate signal")
            return

        # Reserve the asset slot NOW, before any await, so no second signal can
        # slip through the gap. Discarded on rejection or exception below.
        state_provider.pending_assets.add(asset)

        try:
            # Pre-risk validation check using SignalValidator
            strat = signal.get("strategy_type")
            if strat in ["ORBIT_A_240", "ORBIT_A_260"]:
                # ORBIT strategies use built-in filters — validator bypassed by design
                signal["confidence_score"] = 100
            else:
                passed, score, block_reason = signal_validator.validate_signal(signal)
                signal["confidence_score"] = score
                if not passed:
                    signal["outcome"] = "REJECTED"
                    signal["skip_reason"] = f"Validator: REJECTED - {block_reason} (Score: {score})"
                    logger.info(f"[VALIDATOR] {asset} {signal.get('strategy_type')} REJECTED — {block_reason} (Score: {score})")
                    state_provider.pending_assets.discard(asset)
                    await real_db_callback("signals", signal)
                    return

            payload = await risk_engine.process_signal(signal)
            if payload["status"] == "APPROVED":
                signal["outcome"] = "APPROVED"
                db_task = asyncio.create_task(real_db_callback("signals", signal))
                await executor.process_approved_signal(payload, db_task)
            else:
                state_provider.pending_assets.discard(asset)
                signal["outcome"] = "REJECTED"
                signal["skip_reason"] = f"Risk Engine: REJECTED - {payload.get('reason', 'Unknown rejection')}"
                logger.info(f"[RISK] {asset} {signal.get('strategy_type')} REJECTED — {payload.get('reason')}")
                await real_db_callback("signals", signal)
        except Exception as e:
            state_provider.pending_assets.discard(asset)
            raise
            
    signal_engine.risk_engine_callback = handle_signal
    
    # Phase 0 setup
    signal_engine.phase_0_mode = PHASE_0_MODE
    async def handle_phase_0_signal(signal):
        if db is None: return
        
        entry_price = signal.get("yes_price") if signal.get("direction") == "BUY_YES" else signal.get("no_price")
        if not entry_price: entry_price = 0.50
        
        # Calculate dynamic Polymarket taker fee (max 3% at p=0.50)
        distance_from_50 = abs(entry_price - 0.50)
        fee_pct_at_entry = 0.03 * (1 - (distance_from_50 / 0.50))
        fee_pct_at_entry = round(max(0.0, fee_pct_at_entry), 4)

        log_entry = {
            "strategy_type": signal.get("strategy_type"),
            "grade": signal.get("grade"),
            "asset": signal.get("asset"),
            "market_id": signal.get("market_id"),
            "direction": signal.get("direction"),
            "signal_time": signal.get("classified_at_ms"),
            "entry_price_would_have_been": entry_price,
            "fee_pct_at_entry": fee_pct_at_entry
        }
        await db.insert_phase0_log(log_entry)
        logger.info(f"[PHASE 0] Logged hypothetical trade for {signal['asset']} on {signal['strategy_type']}")
        
    signal_engine.phase_0_callback = handle_phase_0_signal

    # 6. Monitor
    monitor = PositionMonitor(sp, db_callback=real_db_callback, telegram_callback=tg.send_alert)

    # 7. Binance Feed (Disabled - replaced by Coinbase SpotFeed)
    sp.binance_feed = None

    # Coinbase spot feed — Chainlink-resolution proxy for Tier 2 margin instrumentation.
    spot_feed = SpotFeed()
    sp.spot_feed = spot_feed

    # Wrap FastAPI run in async task
    port = int(os.environ.get("PORT", 8000))
    config = uvicorn.Config(app, host="0.0.0.0", port=port, log_level="info")
    server = uvicorn.Server(config)
    
    async def run_monitor_loop():
        # Let feeds and DB connect before first evaluation
        await asyncio.sleep(10)
        while True:
            try:
                start_time = time.time()
                await monitor.check_positions()
                latency_ms = int((time.time() - start_time) * 1000)
                sp.monitor_latency = latency_ms
            except Exception as e:
                logger.error(f"Error in monitor loop: {e}")
            # 5s interval: worst-case overshoot on 90s max-hold is 5s (acceptable)
            await asyncio.sleep(5)

    # 8. Discovery Loop: Fetches active BTC/ETH/SOL 5m markets from Gamma API
    async def discover_markets_loop():
        await asyncio.sleep(2)
        while True:
            logger.info("Starting active Polymarket market discovery for 5M crypto markets...")
            try:
                response_data = []
                # Fetch up to 5 pages of events to bypass any stale/expired events at the beginning
                for page in range(5):
                    offset = page * 100
                    url = f"https://gamma-api.polymarket.com/events?limit=100&active=true&closed=false&tag_slug=5M&offset={offset}"
                    req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
                    res = await asyncio.to_thread(urllib.request.urlopen, req, timeout=10.0)
                    page_data = json.loads(res.read().decode('utf-8'))
                    if not page_data:
                        break
                    response_data.extend(page_data)

                candidates = {"BTC": [], "ETH": [], "SOL": []}
                
                for event in response_data:
                    title = event.get("title", "")
                    asset = None
                    if "Bitcoin" in title: asset = "BTC"
                    elif "Ethereum" in title: asset = "ETH"
                    elif "Solana" in title: asset = "SOL"
                    
                    if not asset:
                        continue
                        
                    markets = event.get("markets", [])
                    for m in markets:
                        if not m.get("active") or m.get("closed"):
                            continue
                            
                        # Parse endDate
                        end_date_str = m.get("endDate") or m.get("endDateIso")
                        if not end_date_str: continue
                        
                        try:
                            end_date = datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
                        except Exception:
                            continue
                            
                        now_utc = datetime.now(timezone.utc)
                        time_to_res_sec = (end_date - now_utc).total_seconds()
                        
                        # Filter for active 5M market: must resolve in 10s to 360s from now
                        if not (10 <= time_to_res_sec <= 360):
                            continue
                            
                        # Check outcome prices
                        prices_str = m.get("outcomePrices")
                        if not prices_str: continue
                        try:
                            prices = json.loads(prices_str) if isinstance(prices_str, str) else prices_str
                            if len(prices) < 2: continue
                            p1, p2 = float(prices[0]), float(prices[1])
                            
                            # Allow extreme consensus markets (0.94-0.99) for LAST_SHADOW_V4
                            if not (0.01 < p1 < 0.99 and 0.01 < p2 < 0.99):
                                continue
                        except Exception:
                            continue
                            
                        candidates[asset].append((m, time_to_res_sec, end_date, title))

                # Keep track of active tokens to unsubscribe from old ones
                active_tokens_per_asset = {"BTC": None, "ETH": None, "SOL": None}
                
                for asset in ["BTC", "ETH", "SOL"]:
                    if candidates[asset]:
                        # Sort by time_to_res_sec ASC (closest to resolution first)
                        candidates[asset].sort(key=lambda x: x[1])
                        m, time_to_res_sec, end_date, title = candidates[asset][0]
                        
                        clob_tokens_str = m.get("clobTokenIds")
                        condition_id = m.get("conditionId")
                        if not condition_id or not clob_tokens_str:
                            continue
                            
                        clob_tokens = json.loads(clob_tokens_str) if isinstance(clob_tokens_str, str) else clob_tokens_str
                        if len(clob_tokens) < 2:
                            continue
                            
                        token_id = clob_tokens[0]       # YES/Up outcome (price we track)
                        no_token_id = clob_tokens[1]    # NO/Down outcome (for live BUY_NO orders)
                        res_time_ms = int(end_date.timestamp() * 1000)
                        
                        start_date_str = m.get("startDate") or m.get("startDateIso")
                        start_time_ms = 0
                        if start_date_str:
                            try:
                                start_time_ms = int(datetime.fromisoformat(start_date_str.replace("Z", "+00:00")).timestamp() * 1000)
                            except Exception:
                                pass
                                
                        liquidity = float(m.get("liquidity") or 10000.0)
                        
                        # Market Discovered
                        poly_feed.set_active_market(
                            asset=asset,
                            market_id=condition_id,
                            token_id=token_id,
                            no_token_id=no_token_id,
                            resolution_time_ms=res_time_ms,
                            liquidity_usdc=liquidity,
                            market_open_time=start_time_ms,
                            condition_id=condition_id,
                        )
                        logger.info(f"Discovered active 5M market for {asset}: '{title}' (Token: {token_id}) TTR={time_to_res_sec:.1f}s")
                        active_tokens_per_asset[asset] = token_id

                # Cleanup old tokens
                active_tokens_list = [t for t in active_tokens_per_asset.values() if t is not None]
                sp.poly_feed.cleanup_markets(active_tokens_list)

            except Exception as e:
                logger.error(f"Failed to discover Polymarket 5M markets: {e}")
            
            await asyncio.sleep(30) # Run every 30 seconds

    # 9. Sync System Config Loop
    async def sync_config_loop():
        if db:
            while True:
                try:
                    db_config = await db.load_system_config()
                    for k, v in db_config.items():
                        if v.lower() == "true":
                            sp.config[k] = True
                        elif v.lower() == "false":
                            sp.config[k] = False
                        else:
                            try:
                                sp.config[k] = float(v)
                            except ValueError:
                                sp.config[k] = v
                except Exception as e:
                    logger.error(f"Error syncing system config: {e}")
                await asyncio.sleep(10)

    # 10. Heartbeat Loop
    async def run_heartbeat_loop():
        await asyncio.sleep(15) # Wait for connections to set up
        feed_err_counts = {"BTC": 0, "ETH": 0, "SOL": 0, "Coinbase": 0}
        alerted_errs = {"BTC": False, "ETH": False, "SOL": False, "Coinbase": False}
        
        while True:
            try:
                now_ms = int(time.time() * 1000)
                
                # Check Coinbase connection status
                coinbase_ok = getattr(spot_feed, "connected", False)
                if not coinbase_ok:
                    feed_err_counts["Coinbase"] += 1
                    if feed_err_counts["Coinbase"] >= 5 and not alerted_errs["Coinbase"]:
                        fire_and_forget(tg.send_alert("⚠️ FEED DISCONNECTED — Coinbase WebSocket connection has been down for 5 minutes!"))
                        alerted_errs["Coinbase"] = True
                else:
                    if alerted_errs["Coinbase"]:
                        fire_and_forget(tg.send_alert("🟢 FEED RECOVERED — Coinbase WebSocket connection is now active."))
                        alerted_errs["Coinbase"] = False
                    feed_err_counts["Coinbase"] = 0

                btc_price = 0.0
                eth_price = 0.0
                sol_price = 0.0
                
                if spot_feed.price_history["BTC"]:
                    btc_price = spot_feed.price_history["BTC"][-1][1]
                if spot_feed.price_history["ETH"]:
                    eth_price = spot_feed.price_history["ETH"][-1][1]
                if spot_feed.price_history["SOL"]:
                    sol_price = spot_feed.price_history["SOL"][-1][1]
                    
                btc_delta = get_60s_delta(spot_feed.price_history["BTC"], btc_price, now_ms)
                eth_delta = get_60s_delta(spot_feed.price_history["ETH"], eth_price, now_ms)
                sol_delta = get_60s_delta(spot_feed.price_history["SOL"], sol_price, now_ms)
                
                btc_state = poly_feed.get_market_state("BTC")
                eth_state = poly_feed.get_market_state("ETH")
                sol_state = poly_feed.get_market_state("SOL")
                
                poly_conn_ok = getattr(poly_feed, "connected", False)
                btc_ok = "OK" if poly_conn_ok and btc_state else "ERR"
                eth_ok = "OK" if poly_conn_ok and eth_state else "ERR"
                sol_ok = "OK" if poly_conn_ok and sol_state else "ERR"
                
                # Check Polymarket feed statuses
                for asset, status in [("BTC", btc_ok), ("ETH", eth_ok), ("SOL", sol_ok)]:
                    if status == "ERR":
                        feed_err_counts[asset] += 1
                        if feed_err_counts[asset] >= 5 and not alerted_errs[asset]:
                            fire_and_forget(tg.send_alert(f"⚠️ FEED DISCONNECTED — Polymarket {asset} feed has been down for 5 minutes!"))
                            alerted_errs[asset] = True
                    else:
                        if alerted_errs[asset]:
                            fire_and_forget(tg.send_alert(f"🟢 FEED RECOVERED — Polymarket {asset} feed is now active."))
                            alerted_errs[asset] = False
                        feed_err_counts[asset] = 0

                last_updates = []
                if btc_state: last_updates.append(btc_state["staleness_seconds"])
                if eth_state: last_updates.append(eth_state["staleness_seconds"])
                if sol_state: last_updates.append(sol_state["staleness_seconds"])
                
                last_update_str = f"{min(last_updates):.1f}s" if last_updates else "N/A"
                
                print(
                    f"\n[HEARTBEAT] BTC: ${btc_price:,.2f} (Chg {btc_delta:+.2f}% / 60s) | "
                    f"ETH: ${eth_price:,.2f} (Chg {eth_delta:+.2f}% / 60s) | "
                    f"SOL: ${sol_price:,.2f} (Chg {sol_delta:+.2f}% / 60s)"
                )
                print(
                    f"[HEARTBEAT] Polymarket feeds: BTC {btc_ok} | ETH {eth_ok} | SOL {sol_ok} | "
                    f"Last update: {last_update_str} ago\n"
                )
            except Exception as e:
                logger.error(f"Error in heartbeat loop: {e}")
            await asyncio.sleep(60)

    # DB connection pool keepalive — proactively ensures the pool is healthy
    # every 60 seconds so dashboard reads don't fail after network blips.
    async def db_keepalive_loop():
        while True:
            await asyncio.sleep(60)
            if db:
                try:
                    await db.ensure_pool()
                except Exception as e:
                    logger.error(f"DB keepalive ensure_pool error: {e}")

    async def run_feed_freshness_loop():
        await asyncio.sleep(15) # Wait for startup
        last_alert_time = 0
        while True:
            try:
                if poly_feed.last_message_timestamp > 0:
                    freshness = time.time() - poly_feed.last_message_timestamp
                    if freshness > 10.0:
                        now = time.time()
                        if now - last_alert_time >= 300: # 5 minutes rate limit
                            msg = f"⚠️ FEED STALE — Polymarket WebSocket price feed is stale! No updates for {freshness:.1f} seconds."
                            logger.warning(msg)
                            fire_and_forget(tg.send_alert(msg))
                            last_alert_time = now
            except Exception as e:
                logger.error(f"Error in feed freshness loop: {e}")
            await asyncio.sleep(5)

    # Run everything. Binance is OPTIONAL — it geo-blocks cloud datacenter IPs (HTTP 451)
    # and Last Shadow trades purely off the Polymarket feed, so it's off by default.
    # Set ENABLE_BINANCE_FEED=true to re-enable (e.g. for local dev / ORBIT experiments).
    logger.info("Starting all Phantom V2 sub-systems...")
    coros = [
        discover_markets_loop(),
        poly_feed.run(),
        run_monitor_loop(),
        sync_config_loop(),
        run_heartbeat_loop(),
        db_keepalive_loop(),
        run_feed_freshness_loop(),
        last_shadow_driver(),
        last_shadow_settlement_sweep(),
        phantom_one_driver(),
        phantom_one_settlement_sweep(),
        spot_feed.run(),
        server.serve(),
    ]
    logger.info("Binance feed disabled (replaced by Coinbase SpotFeed).")
    try:
        await asyncio.gather(*coros)
    finally:
        logger.info("Shutting down Phantom V2 sub-systems...")
        if db_writer:
            await db_writer.shutdown()
        if db:
            await db.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Phantom V2 shutting down via KeyboardInterrupt.")
