# Phantom V2 — Production Migration Audit & Tracker

**Objective:** Production hardening (not redevelopment) of the paper-trading system into a
live Polymarket trading platform. v1 ships **Last Shadow (LAST_SHADOW_TRADE_LITE_V4) only** —
the sole strategy with a proven edge (490 trades, 99.6% win, +$196 paper).

- **Workspace:** `C:\projects\phantom_v2\Production` (isolated copy)
- **Original paper system:** `C:\projects\phantom_v2` (untouched backup)
- **Hosting:** Railway (24/7) · **Exchange:** Polymarket CLOB · **DB:** dedicated Postgres (NOT the shared SaaS DB)

---

## Module Audit

### 🟢 CORE — retain as-is
- [ ] `poly_feed.py` — Polymarket WS market-data feed
- [ ] `binance_feed.py` — reference spot prices / heartbeat
- [ ] `database_writer.py` — async DB write queue
- [ ] `strategies/last_shadow_trade_lite_v4.py` — the production strategy (needs execution wiring, see UPDATE)
- [ ] `strategies/utils.py`, `strategies/__init__.py`
- [ ] `telegram.py` — alerting base

### 🟡 UPDATE — required for live trading
- [ ] `executor.py` — **#1 blocker**: build real CLOB order path (currently stubbed pseudo-code) + fee accounting
- [ ] `strategies/last_shadow_trade_lite_v4.py` — route through executor + risk layer (currently hardcodes `is_paper=True`, ignores kill-switch)
- [ ] `risk_engine.py` — live controls: exposure cap, daily-loss halt, kill-switch honored by Last Shadow
- [ ] `main.py` — env-var config, secrets, graceful startup/shutdown
- [ ] `monitor.py` — fee/slippage accounting, on-chain reconciliation
- [ ] `database.py` — dedicated prod Postgres, indexes/migrations
- [ ] `dashboard.py` — remove hardcoded Basic Auth (`admin/Admin@123`); slim to monitoring view

### 🔴 REMOVE — experimental / obsolete / dead  ✅ DONE (Phase 0)
- [x] `archive/` (entire) — deleted
- [x] `strategies/orbit_a_240.py`, `strategies/orbit_a_260.py` — deleted; imports rewired in `strategies/__init__.py`, `signal_engine.py`; test_signal_engine slimmed to Last Shadow
- [x] root dev scripts moved to `scripts/`: check_config, check_trades, reset_db, update_bankroll, test_analytics
- [x] `tests/` ad-hoc scratch removed (check_signals*, fetch_poly, clear_db, verify_*, query_approved_signals, check_stats)
- NOTE: inert ORBIT config entries remain in `risk_engine.py` (VALID_TTR_WINDOWS, CHECK 4/7) and `main.py` (_STRATEGY_TTR_CEILINGS). They never trigger (no ORBIT signals generated). Clean up in Phase 2 alongside the risk-engine live refactor.

### 🔵 DEFER — future releases
- ORBIT strategies (re-add once proven) · `analytics_engine.py` deep-analytics · Phase-0 edge logging ·
  `strategy_stats.py` (simplify) · `signal_validator.py` (decide in Phase 1 live-flow design) · multi-strategy orchestration

---

## Production Gap Analysis

| Capability | Status | Action |
|---|---|---|
| Live order execution | ❌ Stubbed | Build CLOB executor |
| Fee/slippage accounting | ❌ Missing | **Validate Last Shadow edge survives real costs — DO FIRST** |
| Risk mgmt (exposure/daily-loss/kill) | ⚠️ Bypassed by Last Shadow | Enforce on live path |
| Security (secrets, dashboard creds) | ❌ Hardcoded | Env-var secrets, remove creds |
| Config management | ⚠️ Mixed DB/env | Env-driven for Railway |
| Failure recovery | ⚠️ Manual restarts | Auto-restart + state recovery + reconciliation |
| Prod logging | ⚠️ Basic | Structured logs + rotation |
| Monitoring/alerting | ⚠️ Partial | Health check + balance/crash alerts |
| DB optimization | ⚠️ Shared DB | Dedicated Postgres + indexes |
| Deployment | ❌ None | Railway config + health endpoint |

---

## Phased Plan

- **Phase 0 — Cleanup** ✅ COMPLETE: deleted `archive/` + ORBIT, moved dev scripts to `scripts/`, pruned ad-hoc tests. 21 tests pass, clean imports, Last-Shadow-only.
- **Phase 1 — Decisive test:** CLOB executor **with fee/slippage recording** → live-tiny → confirm edge survives costs.
  - [x] `clob_executor.py` — uniform `place_order()`; paper simulates, live posts a real FAK market order via py-clob-client; records requested vs fill price, fee, slippage (`FillResult`). Paper path unit-tested (`tests/test_clob_executor.py`). Creds from env only.
  - [x] `.gitignore` + `.env.example` hardened (secrets never committed).
  - [x] Execution journal (`execution_journal.py`): `execution_journal` table + `record_fill()`; auto-creates table, indexed by strategy/is_paper.
  - [x] Wired Last Shadow `_fill_order` → `ClobExecutor.place_order`; position now booked at ACTUAL fill price + `is_paper` from result; cost row journaled. Paper e2e verified. 23 tests pass.
  - [x] Dual token_ids plumbed: `set_active_market(no_token_id=...)` stores `yes_token_id`/`no_token_id` in `market_state`; `main.py` discovery passes `clobTokenIds[1]`; Last Shadow captures both and `_fill_order` buys the YES token for BUY_YES / NO token for BUY_NO. Verified end-to-end. (Convention YES=[0]/NO=[1] — confirm on first live order via `FillResult.raw`.)
  - [ ] **PRE-LIVE**: startup CLOB `health()` check + balance/allowance probe; verify first live order response parsing against `FillResult.raw`.
  - [ ] Live-tiny run → `SELECT ... FROM execution_journal` → analyze slippage/fee vs edge → GO/NO-GO.
- **Phase 2 — Safety:** risk controls + kill switch + security + env config.
- **Phase 3 — Deploy:** Railway + dedicated Postgres + monitoring + failure recovery.
- **Phase 4 — Scale:** validate live P&L vs paper, then increase size.

---

## Inputs still needed from owner
- Wallet address (public 0x…) · CLOB API creds generated · USDC balance · Railway account ready
- Decisions: per-trade size (live), max concurrent exposure, assets (BTC/ETH/SOL vs BTC-only), daily-loss limit
- Confirm jurisdiction allows Polymarket
- **Never share the private key / API secret in chat — Railway env vars only**
