import asyncio
import asyncpg
import os
import json
from datetime import datetime, timezone
import math
from dotenv import load_dotenv
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("AnalyticsEngine")

class AnalyticsEngine:
    def __init__(self):
        load_dotenv()
        self.db_url = os.getenv("DATABASE_URL")
        self.trades = []
    
    async def run_analysis(self, output_file="daily_report.md", return_data=False):
        await self.extract_trade_journals()
        
        if not self.trades:
            logger.warning("No trades found in database.")
            return None if return_data else None
            
        stats = self.calculate_strategy_stats()
        rca = self.perform_root_cause_analysis()
        win_patterns = self.analyze_win_patterns()
        recommendations = self.generate_recommendations(stats, rca, win_patterns)
        market_cond = self.analyze_market_conditions()
        ranking = self.rank_strategies(stats)
        
        if output_file:
            self.generate_report(stats, rca, win_patterns, recommendations, market_cond, ranking, output_file)
            
        if return_data:
            return {
                "stats": stats,
                "rca": rca,
                "win_patterns": win_patterns,
                "recommendations": recommendations,
                "market_cond": market_cond,
                "ranking": ranking
            }
        
    async def extract_trade_journals(self):
        """Phase 1: Complete Trade Journal - Extracts and shapes the data."""
        query = """
            SELECT 
                p.id as trade_id,
                p.opened_at as entry_time,
                p.closed_at as exit_time,
                p.market_id,
                p.asset,
                p.strategy_type as strategy,
                p.direction as side,
                p.entry_price,
                p.exit_price,
                p.size_usdc as position_size,
                p.pnl as profit_loss,
                p.close_reason,
                p.signal_to_fill_ms as execution_latency_ms,
                s.yes_price,
                s.no_price,
                s.spread as spread_at_entry,
                s.time_to_resolution_seconds as ttr_seconds
            FROM positions p
            JOIN signals s ON p.signal_id = s.id
            WHERE p.status = 'CLOSED'
        """
        
        try:
            conn = await asyncpg.connect(self.db_url)
            rows = await conn.fetch(query)
            await conn.close()
            
            for row in rows:
                pnl = row['profit_loss']
                if pnl > 0:
                    result = "WIN"
                elif pnl < 0:
                    result = "LOSS"
                else:
                    result = "BREAKEVEN"
                    
                profit_loss_pct = (pnl / row['position_size']) * 100 if row['position_size'] > 0 else 0
                
                hold_seconds = 0
                if row['exit_time'] and row['entry_time']:
                    hold_seconds = (row['exit_time'] - row['entry_time']).total_seconds()
                    
                # We don't have prev_yes_price directly in schema, but we can infer momentum delta from entry constraints
                # For now, we leave it at 0.0 or add a placeholder.
                # In the real strategies we added `momentum` to the signal payload, but unless we altered schema.sql
                # to save it, it might not be there. Let's see if we can parse it from `skip_reason` or similar, 
                # but since it's an approved signal, it's not in skip_reason. We will just set it to 0.0 for now
                # and maybe add a column later if needed.
                
                trade = {
                    "trade_id": str(row['trade_id']),
                    "market_id": row['market_id'],
                    "asset": row['asset'],
                    "strategy": row['strategy'],
                    "side": row['side'],
                    "entry_price": row['entry_price'],
                    "exit_price": row['exit_price'],
                    "position_size": row['position_size'],
                    "entry_time": row['entry_time'],
                    "exit_time": row['exit_time'],
                    "hold_seconds": hold_seconds,
                    "profit_loss": pnl,
                    "profit_loss_pct": round(profit_loss_pct, 2),
                    "result": result,
                    "yes_price": float(row['yes_price']) if row['yes_price'] is not None else 0.5,
                    "no_price": float(row['no_price']) if row['no_price'] is not None else 0.5,
                    "ttr_seconds": int(row['ttr_seconds']) if row['ttr_seconds'] is not None else 0,
                    "tp_triggered": row['close_reason'] == 'TAKE_PROFIT',
                    "sl_triggered": row['close_reason'] == 'STOP_LOSS',
                    "timeout_triggered": row['close_reason'] == 'TIMEOUT',
                    "spread_at_entry": float(row['spread_at_entry']) if row['spread_at_entry'] is not None else 0.0,
                    "execution_latency_ms": row['execution_latency_ms'],
                    "close_reason": row['close_reason']
                }
                self.trades.append(trade)
                
            logger.info(f"Extracted {len(self.trades)} trades for analysis.")
            
        except Exception as e:
            logger.error(f"Error extracting trades: {e}")


    async def generate_deep_analysis(self, limit=1000):
        query = '''
            SELECT 
                p.id::text as trade_id,
                p.opened_at as entry_time,
                p.closed_at as exit_time,
                p.market_id,
                p.asset,
                p.strategy_type as strategy,
                p.direction as side,
                p.entry_price,
                p.exit_price,
                p.size_usdc as position_size,
                p.pnl as profit_loss,
                p.close_reason,
                p.signal_to_fill_ms as execution_latency_ms,
                s.yes_price,
                s.no_price,
                s.spread as spread_at_entry,
                s.time_to_resolution_seconds as ttr_seconds,
                s.velocity_count,
                s.magnitude_pct
            FROM positions p
            JOIN signals s ON p.signal_id = s.id
            WHERE p.status = 'CLOSED'
            ORDER BY p.opened_at DESC
            LIMIT $1
        '''
        
        try:
            conn = await asyncpg.connect(self.db_url)
            rows = await conn.fetch(query, limit)
            await conn.close()
            
            deep_trades = []
            for row in rows:
                t = dict(row)
                pnl = float(t['profit_loss'] or 0)
                dur = 0
                if t['exit_time'] and t['entry_time']:
                    dur = (t['exit_time'] - t['entry_time']).total_seconds()
                t['duration'] = dur
                t['result'] = "WIN" if pnl > 0 else ("BREAKEVEN" if pnl == 0 else "LOSS")
                
                # Intelligent Tagging
                ttr = t.get('ttr_seconds')
                if ttr is None: ttr = 0
                vel = t.get('velocity_count')
                if vel is None: vel = 0
                spread = t.get('spread_at_entry')
                if spread is None: spread = 0.0
                
                reason = "UNKNOWN"
                if pnl <= 0:
                    if ttr > 250: reason = "Early Entry"
                    elif vel > 15: reason = "Volatility Spike"
                    elif dur < 20 and spread > 0.02: reason = "Tight Stop Loss"
                    else: reason = "Market Reversal"
                else:
                    if dur < 30: reason = "Strong Momentum Continuation"
                    elif ttr < 10: reason = "Late Settlement Advantage"
                    else: reason = "Perfect Timing"
                
                t['intelligence_classification'] = reason
                
                for k, v in t.items():
                    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                        t[k] = 0.0
                    if isinstance(v, datetime):
                        t[k] = v.isoformat()
                    import decimal
                    if isinstance(v, decimal.Decimal):
                        t[k] = float(v)
                deep_trades.append(t)
            
            return deep_trades
            
        except Exception as e:
            logger.error(f"Error generating deep analysis: {e}")
            return []

    def calculate_strategy_stats(self):
        """Phase 2 & 7: Dashboard and Quant Metrics"""
        stats = {}
        for t in self.trades:
            strat = t["strategy"]
            if strat not in stats:
                stats[strat] = {
                    "trades": 0, "wins": 0, "losses": 0, "pnl": 0.0,
                    "gross_profit": 0.0, "gross_loss": 0.0,
                    "total_hold_time": 0, "yes_trades": 0, "yes_wins": 0,
                    "no_trades": 0, "no_wins": 0,
                    "largest_win": 0.0, "largest_loss": 0.0,
                    "current_streak": 0, "longest_win_streak": 0, "longest_loss_streak": 0,
                    "peak_pnl": 0.0, "max_drawdown": 0.0, "cumulative_pnl": 0.0
                }
            
            s = stats[strat]
            s["trades"] += 1
            s["pnl"] += t["profit_loss"]
            s["total_hold_time"] += t["hold_seconds"]
            
            # Drawdown tracking
            s["cumulative_pnl"] += t["profit_loss"]
            if s["cumulative_pnl"] > s["peak_pnl"]:
                s["peak_pnl"] = s["cumulative_pnl"]
            dd = s["peak_pnl"] - s["cumulative_pnl"]
            if dd > s["max_drawdown"]:
                s["max_drawdown"] = dd
                
            if t["side"] == "BUY_YES":
                s["yes_trades"] += 1
            else:
                s["no_trades"] += 1
                
            if t["result"] == "WIN":
                s["wins"] += 1
                s["gross_profit"] += t["profit_loss"]
                s["largest_win"] = max(s["largest_win"], t["profit_loss"])
                if s["current_streak"] < 0: s["current_streak"] = 0
                s["current_streak"] += 1
                s["longest_win_streak"] = max(s["longest_win_streak"], s["current_streak"])
                if t["side"] == "BUY_YES": s["yes_wins"] += 1
                if t["side"] == "BUY_NO": s["no_wins"] += 1
            elif t["result"] == "LOSS":
                s["losses"] += 1
                s["gross_loss"] += abs(t["profit_loss"])
                s["largest_loss"] = min(s["largest_loss"], t["profit_loss"])
                if s["current_streak"] > 0: s["current_streak"] = 0
                s["current_streak"] -= 1
                s["longest_loss_streak"] = max(s["longest_loss_streak"], abs(s["current_streak"]))
                
        # Calculate derived metrics
        for k, v in stats.items():
            v["win_rate"] = (v["wins"] / v["trades"]) * 100 if v["trades"] > 0 else 0
            v["avg_trade"] = v["pnl"] / v["trades"] if v["trades"] > 0 else 0
            v["avg_win"] = v["gross_profit"] / v["wins"] if v["wins"] > 0 else 0
            v["avg_loss"] = (v["gross_loss"] / v["losses"]) * -1 if v["losses"] > 0 else 0
            v["profit_factor"] = (v["gross_profit"] / v["gross_loss"]) if v["gross_loss"] > 0 else float('inf')
            v["avg_hold"] = v["total_hold_time"] / v["trades"] if v["trades"] > 0 else 0
            v["yes_win_rate"] = (v["yes_wins"] / v["yes_trades"]) * 100 if v["yes_trades"] > 0 else 0
            v["no_win_rate"] = (v["no_wins"] / v["no_trades"]) * 100 if v["no_trades"] > 0 else 0
            
            # Simplified Sharpe (Assuming 0% risk free, annualizing based on per-trade avg)
            v["expectancy"] = (v["win_rate"]/100 * v["avg_win"]) + ((1 - v["win_rate"]/100) * v["avg_loss"])
            
        return stats

    def perform_root_cause_analysis(self):
        """Phase 3: Tag losing trades with root causes"""
        rca = {}
        for t in self.trades:
            if t["result"] == "LOSS":
                strat = t["strategy"]
                if strat not in rca:
                    rca[strat] = []
                    
                reason = "MARKET_NOISE"
                desc = "Normal market fluctuation."
                
                if t.get("timeout_triggered") or t.get("close_reason") == "TIME_EXIT":
                    reason = "TIMEOUT_EXIT"
                    desc = "Trade held too long and hit max time limit."
                elif t["sl_triggered"]:
                    if t["hold_seconds"] < 20:
                        reason = "FAKE_BREAKOUT"
                        desc = "Price reversed immediately after entry."
                    else:
                        reason = "MOMENTUM_EXHAUSTION"
                        desc = "Price stalled and slowly bled into stop loss."
                elif t["ttr_seconds"] < 30:
                    reason = "LATE_ENTRY"
                    desc = "Entered too close to resolution, leaving no time to recover."
                elif t["spread_at_entry"] and t["spread_at_entry"] > 0.03:
                    reason = "SPREAD_TOO_WIDE"
                    desc = "Slippage/Spread made it difficult to secure profit."
                    
                rca[strat].append({
                    "trade_id": t["trade_id"],
                    "entry": t["entry_price"],
                    "exit": t["exit_price"],
                    "pnl": t["profit_loss"],
                    "reason": reason,
                    "desc": desc
                })
        return rca

    def analyze_win_patterns(self):
        """Phase 4: Analyze winning trades"""
        patterns = {}
        for t in self.trades:
            if t["result"] == "WIN":
                strat = t["strategy"]
                if strat not in patterns:
                    patterns[strat] = {"entries": [], "holds": [], "ttrs": []}
                    
                patterns[strat]["entries"].append(t["entry_price"])
                patterns[strat]["holds"].append(t["hold_seconds"])
                patterns[strat]["ttrs"].append(t["ttr_seconds"])
                
        # Averages
        result = {}
        for strat, data in patterns.items():
            if len(data["entries"]) > 0:
                result[strat] = {
                    "avg_entry": sum(data["entries"]) / len(data["entries"]),
                    "avg_hold": sum(data["holds"]) / len(data["holds"]),
                    "avg_ttr": sum(data["ttrs"]) / len(data["ttrs"])
                }
        return result

    def analyze_market_conditions(self):
        """Phase 6: Market Conditions"""
        # A basic stub for market conditions based on TTR and Side
        conds = {
            "fast_market_winrate": 0, "slow_market_winrate": 0,
            "late_ttr_winrate": 0, "early_ttr_winrate": 0
        }
        
        fast_wins = fast_total = 0
        slow_wins = slow_total = 0
        
        for t in self.trades:
            # Proxies: short hold = fast market, long hold = slow market
            if t["hold_seconds"] < 15:
                fast_total += 1
                if t["result"] == "WIN": fast_wins += 1
            else:
                slow_total += 1
                if t["result"] == "WIN": slow_wins += 1
                
        if fast_total > 0: conds["fast_market_winrate"] = (fast_wins / fast_total) * 100
        if slow_total > 0: conds["slow_market_winrate"] = (slow_wins / slow_total) * 100
        
        return conds

    def rank_strategies(self, stats):
        """Phase 8: Leaderboard"""
        rankings = []
        for strat, s in stats.items():
            # Score out of 10: 50% Win Rate, 50% Profit Factor
            wr_score = min(5, (s["win_rate"] / 80) * 5) # 80% WR = 5 pts
            pf_score = min(5, (s["profit_factor"] / 3) * 5) # 3.0 PF = 5 pts
            total_score = round(wr_score + pf_score, 1)
            
            if total_score >= 8.0:
                rec = "PROMOTE / ALLOCATE MORE"
            elif total_score >= 5.0:
                rec = "KEEP TESTING"
            else:
                rec = "REVIEW OR RETIRE"
                
            rankings.append({
                "strategy": strat,
                "win_rate": s["win_rate"],
                "net_pnl": s["pnl"],
                "profit_factor": s["profit_factor"],
                "sharpe_ratio": s.get("expectancy", 0.0),
                "score": total_score,
                "recommendation": rec
            })
            
        rankings.sort(key=lambda x: x["score"], reverse=True)
        return rankings

    def generate_recommendations(self, stats, rca, win_patterns):
        """Phase 5 & 9: Auto recommendations"""
        recs = []
        for strat, s in stats.items():
            if strat in win_patterns:
                wp = win_patterns[strat]
                recs.append(f"Modify **{strat}** optimal entry target closer to {round(wp['avg_entry'], 2)}.")
                recs.append(f"Consider adjusting **{strat}** max hold time closer to {round(wp['avg_hold'], 0)}s based on win averages.")
            
            # Analyze RCA
            if strat in rca:
                fake_breakouts = len([r for r in rca[strat] if r['reason'] == 'FAKE_BREAKOUT'])
                if fake_breakouts > (s['losses'] * 0.4):
                    recs.append(f"**{strat}** is suffering from fake breakouts. Increase momentum threshold or wait for confirmation.")
                    
            if s["no_win_rate"] > s["yes_win_rate"] + 10:
                recs.append(f"Increase BUY_NO allocation for **{strat}** (Significantly higher win rate).")
                
        return recs

    def generate_report(self, stats, rca, win_patterns, recommendations, conds, ranking, output_file):
        """Phase 10: Generate Markdown Report"""
        lines = []
        lines.append("# PHANTOM DAILY REPORT & STRATEGY OPTIMIZATION")
        lines.append(f"*Generated at {datetime.now(timezone.utc).isoformat()}*\n")
        
        # Overall Summary
        total_trades = sum(s["trades"] for s in stats.values())
        total_wins = sum(s["wins"] for s in stats.values())
        total_losses = sum(s["losses"] for s in stats.values())
        total_pnl = sum(s["pnl"] for s in stats.values())
        win_rate = (total_wins / total_trades) * 100 if total_trades > 0 else 0
        
        best_strat = ranking[0]["strategy"] if ranking else "N/A"
        worst_strat = ranking[-1]["strategy"] if ranking else "N/A"
        
        lines.append("## Overall Session Summary")
        lines.append(f"- **Total Trades:** {total_trades}")
        lines.append(f"- **Wins:** {total_wins}")
        lines.append(f"- **Losses:** {total_losses}")
        lines.append(f"- **Win Rate:** {round(win_rate, 2)}%")
        lines.append(f"- **Total PnL:** ${round(total_pnl, 2)}")
        lines.append(f"- **Best Strategy:** {best_strat}")
        lines.append(f"- **Worst Strategy:** {worst_strat}\n")
        
        # Leaderboard
        lines.append("## Strategy Leaderboard")
        for i, r in enumerate(ranking):
            lines.append(f"### {i+1}. {r['strategy']}")
            lines.append(f"- **Score:** {r['score']}/10.0")
            lines.append(f"- **Win Rate:** {round(r['win_rate'], 1)}%")
            lines.append(f"- **Profit Factor:** {round(r['profit_factor'], 2)}")
            lines.append(f"- **Total PnL:** ${round(r['pnl'], 2)}")
            lines.append(f"- **System Recommendation:** {r['recommendation']}\n")
            
        # Strategy Deep Dives
        lines.append("## Strategy Deep Dives")
        for strat, s in stats.items():
            lines.append(f"### {strat}")
            lines.append(f"- **Trades:** {s['trades']} | **Wins:** {s['wins']} | **Losses:** {s['losses']}")
            lines.append(f"- **Max Drawdown:** ${round(s['max_drawdown'], 2)}")
            lines.append(f"- **Avg Winner:** ${round(s['avg_win'], 2)} | **Avg Loser:** ${round(s['avg_loss'], 2)}")
            lines.append(f"- **Avg Hold Time:** {round(s['avg_hold'], 1)} seconds")
            lines.append(f"- **Longest Win Streak:** {s['longest_win_streak']}")
            lines.append(f"- **BUY_YES Win Rate:** {round(s['yes_win_rate'], 1)}% | **BUY_NO Win Rate:** {round(s['no_win_rate'], 1)}%\n")
            
            # Root Cause
            if strat in rca:
                lines.append(f"#### Loss Analysis (Sample)")
                for r in rca[strat][:3]: # show up to 3
                    lines.append(f"- Trade {r['trade_id'][:8]}: **{r['reason']}** (${round(r['pnl'], 2)}) - {r['desc']}")
            lines.append("\n")
            
        # Recommendations
        lines.append("## Automatic Improvement Recommendations")
        for idx, rec in enumerate(recommendations):
            lines.append(f"{idx+1}. {rec}")
            
        lines.append("\n## Market Conditions Analysis")
        lines.append(f"- **Fast Market (Hold < 15s) Win Rate:** {round(conds['fast_market_winrate'], 1)}%")
        lines.append(f"- **Slow Market (Hold > 15s) Win Rate:** {round(conds['slow_market_winrate'], 1)}%")
        
        with open(output_file, "w") as f:
            f.write("\n".join(lines))
            
        logger.info(f"Analysis complete. Report written to {output_file}")

if __name__ == "__main__":
    engine = AnalyticsEngine()
    asyncio.run(engine.run_analysis())
