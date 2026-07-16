# Tier 2 Plan — Conviction Filter, Latency, and Live Validation

**Objective:** Convert Last Shadow from "thin-yield carry with a fat tail" into a
higher-yield, tail-protected strategy by reading the market's own resolution source,
and validate the live edge before scaling.

**Key discovery (verified from live Gamma metadata):** BTC/ETH/SOL 5-min markets
resolve on **Chainlink Data Streams** (`data.chain.link/streams/{btc,eth,sol}-usd`),
rule: **Up if price at window close ≥ price at window open**. A reversal = the asset
sitting at the open price while the order book is overconfident. Tier 2 detects that
gap directly instead of guessing.

**Two real bottlenecks Tier 2 attacks** (everything is judged against these):
1. **The tail** — one reversal costs ~80 wins (avg win $0.63 vs loss $50; breakeven
   reversal rate 1.23%, observed 0.28%).
2. **Live execution** — fees/slippage on a $0.63 edge. `takerBaseFee=1000` seen in
   market metadata (may be placeholder) — **must be verified on a real fill**.

**Design decisions (locked):**
- Path B first: **Coinbase WS spot as resolution proxy** (Binance geo-blocked on
  Railway; Chainlink Data Streams gated/unknown cost → evaluate later as 2d).
- **Instrument first, gate second** — margins logged on every trade before any
  entry-gating; thresholds set from measured data, not guesses.
- The margin filter's main payoff is **yield expansion** (enter 0.93–0.96 when margin
  is decisive → 4–7× yield/trade), reversal-avoidance is the bonus.
- No fusion engine / multi-agent scoring — two inputs only (resolution proxy +
  book depth) into the existing driver.
- Speed target: **event-driven decision loop, ~100–300ms tick→order end-to-end.**
  Microsecond HFT explicitly out of scope (edge is selection, not speed).

---

## Phases

### Phase 0-pre — Risk wiring  ✅ DONE (Task 1)
- [x] `PER_TRADE_USDC` overrides hardcoded $50 size
- [x] `KILL_SWITCH` (env + DB config) halts new entries
- [x] `MAX_CONCURRENT_USDC` exposure cap
- [x] `DAILY_LOSS_LIMIT_USDC` daily halt (30s-cached realized pnl)
- [x] PnL/exposure use actual per-trade size · 6 tests · 29 pass
- Rationale: without this the "$5 probe" would have placed $50 orders with no
  kill switch. Must exist before any `PAPER_MODE=false`.

### Phase 0-region — Move Railway `sfo` → `us-east`  (USER, do BEFORE probe)
- [ ] So the fee probe measures the final-config latency, not sfo's ~150ms-worse RTT.
- [ ] 5-minute change in Railway service Settings → Region.

### Phase 0 — Live-tiny fee probe  ⛔ GATES EVERYTHING
- [ ] Owner: regenerate Polymarket API creds (old ones exposed) → Railway vars
- [ ] Owner: small USDC balance in wallet (~$25)
- [ ] Prep checklist: PER_TRADE_USDC=5, PAPER_MODE=false, kill-switch ready
- [ ] Run 3–5 real trades; inspect `execution_journal` + raw fill responses
- [ ] **Answer: real taker fee? real slippage at 0.99? edge survives?** → GO/NO-GO
- [ ] Verify YES=clobTokenIds[0] convention on first live fill

### Phase 1 — Latency quick wins (no strategy change)
- [ ] (region move handled in Phase 0-region above)
- [x] Latency instrumentation: journal records `decision_staleness_ms` (tick→decision)
      alongside `latency_ms` (decision→fill) — gives end-to-end loop time per trade.
- [ ] **DEFERRED — event-driven driver refactor.** Judgment: Last Shadow has ~6s of
      execution runway, so 0.5s polling catches every entry; the dominant latency is
      order RTT (fixed by the region move). Per "measure first", we instrument now and
      only do the callback refactor if `decision_staleness_ms` data shows poll lag
      actually costs fills. Revisit if we later shrink the entry window to TTR 1–3s.
- [ ] Persistent CLOB session + pre-warmed order path (do alongside Phase 0 probe)

### Phase 2a — Instrumentation ✅ DONE (Task 4) — calibration clock starts on deploy
- [x] `spot_feed.py`: Coinbase WS (BTC/ETH/SOL-USD), reconnect, degrades safely
- [x] Window-open capture from the tick stream (first tick of each 5-min window)
- [x] Per-fill journal columns: `spot_open`, `spot_now`, `margin_pct`, `spot_agrees`
      (+ `decision_staleness_ms`) — idempotent ALTERs migrate the live table
- [x] NO behavior change — Last Shadow trades exactly as today; margin is logged only
- [ ] Accumulate 5–7 days of margin data before Phase 2b calibration
- [ ] (later refinement) also log margin for SKIPPED windows, not just fills

### Phase 2b — Conviction gate + yield expansion (needs 5–7 days of 2a data)
- [ ] Calibrate: margin distribution of winners vs any losers; last-5s noise band
- [ ] Gate: trade only if spot direction agrees with favorite AND |margin| ≥ threshold
- [ ] Widen entry band (e.g. ≥0.93) when margin is decisive; keep 0.99-only otherwise

### Reversal circuit-breaker — REQUIRED before sustained/unattended live trading
- [ ] Auto-pause Last Shadow on 2 reversals within 30 min (protects any live trading,
      not just the conviction filter — moved out of 2b). OK to skip only for the
      supervised 3–5 trade Phase-0 probe where you're watching it live.

### Phase 2c — Slippage guard
- [ ] Pre-fire `get_order_book` depth check at target price; skip/size-down on thin book

### Phase 2d — Chainlink Data Streams (conditional)
- [ ] Research access/cost/latency in parallel — build ONLY if the Coinbase proxy
      demonstrably fails at margins that matter

---

## Timeline (elapsed, not build — code is ~3–4 sessions total)
- **Week 1:** Phase 0 answered, Phase 1 done, 2a deployed and logging
- **Week 2:** 2b calibrated and live, 2c live → Tier 2 operational
- **Week 3:** judge on data; decide Data Streams + scaling

## Explicitly rejected (and why)
- Avoiding the last 10s — that *is* the strategy; later entry = less risk, not more
- Order-flow as direction predictor — price already aggregates flow; book data is
  used for execution quality only
- Next-window pending-order tracking — no value for a final-seconds strategy
- Multi-source fusion/confidence engine — unvalidatable against 0.3% events;
  complexity without measurable payoff at current scale
