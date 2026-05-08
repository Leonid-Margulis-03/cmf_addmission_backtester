# ROADMAP.md — Improvement Roadmap

1. **`k` calibration from `trades.csv`**

   The order-arrival intensity parameter `k` is currently hardcoded to `1e6` across all sweep configs. The paper derives `k` by fitting `ln λ(δ) = ln A − k·δ` to the empirical distribution of trade prices relative to the dealer's quotes. This fit is asset- and price-scale dependent: the original paper used `k ≈ 1.5` on assets priced around $100; this asset trades near $0.01, so the correct `k` is roughly `1e6` to maintain the same dimensionless spread-to-price ratio. Without calibration, the spread formula produces quotes that are either too tight (fills too fast, inventory explodes) or too wide (almost no fills). See REPORT.md for sweep evidence of this sensitivity.

2. **Fee model**

   Every fill is currently credited at the full quoted price with no transaction cost. Real exchanges charge taker fees (e.g., +5 bp) and sometimes pay maker rebates (e.g., −1 bp). Ignoring fees overstates PnL on every fill and can make losing configurations appear profitable. The fix is a single per-fill adjustment in `TradeUpdate`: add or subtract `fee_rate * fill_price * size` depending on whether the fill is passive (maker) or active (taker).

3. **Forced inventory liquidation at session boundary T**

   The paper assumes the dealer flattens inventory at time T. The current code rolls T forward (`T = t + horizon_seconds_`) whenever the session expires, so inventory carries across indefinitely. This means the inventory penalty `q·γ·σ²·(T−t)` resets to zero at each boundary rather than forcing liquidation. The correct behavior is to issue a market order (or use the prevailing mid as a liquidation price) when `t >= T_old`, then reset `q = 0` before starting the next session. This would reduce end-of-session inventory risk and align PnL accounting with the paper's assumptions.

4. **Partial fills and queue-position model**

   The current fill rule (`TradeUpdate`) treats every incoming aggressor trade as fully consuming the dealer's resting quote regardless of the trade size. In reality, the dealer's order sits at a position in the queue; only a portion of each aggressor trade reaches our level. A FIFO or pro-rata model would track the depth-ahead-of-us at the time of quote placement, reduce that depth on each aggressor trade at our level, and only credit a fill when depth-ahead reaches zero. This would lower apparent fill rates and bring PnL estimates closer to what live execution would achieve.

5. **Time-window slice runs**

   `replay()` currently processes the entire dataset unconditionally. Adding `[start_us, end_us]` parameters would let a researcher evaluate the strategy on specific market regimes (e.g., first hour only, or a known volatility spike). This requires a minor change to `replay()`: skip `BookUpdate`/`TradeUpdate` calls until `local_timestamp >= start_us` and break when `local_timestamp > end_us`. No data-loading changes are needed; the CSV readers already parse timestamps.

6. **Multi-realization Monte Carlo**

   The paper validates results over 1000 simulated price paths. With historical data, the closest analog is to split the dataset into N non-overlapping time chunks and run the strategy independently on each. Reporting mean ± std of PnL and terminal inventory across chunks gives a much more reliable signal than a single replay. This requires `replay()` to accept time bounds (see item 5) and a thin outer loop to aggregate results.

7. **Regime filter**

   The PnL plot shows drawdown episodes that coincide with sharp mid-price moves. When σ jumps suddenly (e.g., exceeds 2× its rolling mean), the A–S spread formula widens automatically, but the widening lags the shock by one update. A regime filter would detect the σ spike and immediately cancel or pause quoting for a short cooldown period, avoiding adverse selection from the initial move. Implementation: compare `GetSigma()` to an EWMA of recent σ values; if the ratio exceeds a threshold, set `my_bid.active = my_ask.active = false` for the next K updates.

8. **Better σ estimator**

   The current estimator is plain realized variance: `σ² = Σ(Δs)² / Σ(Δt)`. For HFT tick data, microstructure noise (bid-ask bounce, rounding) inflates this estimate significantly. Alternatives include: EWMA (exponentially downweights older ticks), two-scale realized volatility (separates signal from noise using two sampling frequencies), or the Barndorff-Nielsen–Shephard bipower variation (robust to jumps). Any of these would improve the quality of σ fed into the spread formula and reduce over-quoting during low-signal periods.

9. **`last_s == -1` sentinel hardening**

   `VolatilityHandler::Update` initializes `last_s` to `-1` and checks for that value to detect the first call (main.cpp:66–70). This pattern breaks silently if the mid-price ever equals exactly `−1.0`, which is impossible for a positive-price asset but is still a fragile design. The correct fix is to add a `bool initialized_ = false` member, set it to `true` on the first call, and replace the sentinel check with `if (!initialized_)`. This is a one-line change with no behavioral impact under normal conditions.

10. **Tick-size and price-grid awareness**

    Quote prices computed from the A–S formulas are continuous-valued real numbers. Real exchanges require orders to be placed on a discrete price grid (tick size). When the strategy's computed ask price falls between two ticks, it should round up (to avoid being inside the spread) and the bid should round down. Without this, quote prices may coincide with the inside spread and produce unrealistic fills. The fix requires knowing the tick size for the asset (derivable from the minimum non-zero price difference in `MD/lob.csv`) and applying `ceil`/`floor` rounding in `BookUpdate` before setting `my_ask.price` and `my_bid.price`.
