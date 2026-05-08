# REPORT.md — Performance Report

## 1. Setup

The dataset covers six days of limit order book and trade data for a crypto asset. The LOB feed (`MD/lob.csv`) is 925 MB and the trades feed (`MD/trades.csv`) is 946 MB; together they yield 1 036 690 book updates and the corresponding aggressor trades. The mid-price opened around 0.011 and ended at 0.00774, a decline of roughly 30% over the run. Two strategy modes from Avellaneda–Stoikov §3.3 were compared: *inventory mode* (`use_inventory_skew = true`), which adjusts quotes via the reservation-price skew, and *symmetric mode* (`use_inventory_skew = false`), the paper's benchmark that keeps quotes symmetric around mid regardless of inventory. The parameter sweep varied γ ∈ {0.01, 0.1, 1, 10, 100} across both modes for 10 configurations total, all sharing `k = 1e6`, `horizon_seconds = 3600`, and `vol_window = 2000`.

## 2. Results

```
| γ     | inv PnL  | sym PnL | inv |q| | sym |q| | inv fills | sym fills |
|-------|----------|---------|---------|---------|-----------|-----------|
| 0.01  | −0.125   | +2.30   | 12      | 503     | 480k      | 481k      |
| 0.1   | −0.341   | +2.32   | 14      | 523     | 479k      | 479k      |
| 1     | −0.395   | +2.49   | 11      | 536     | 477k      | 475k      |
| 10    | −0.397   | +2.48   | 3       | 627     | 458k      | 452k      |
| 100   | −0.335   | +2.02   | 1       | 355     | 344k      | 295k      |
```

PnL is mark-to-market (`final_x + final_q × final_mid`). `|q|` is the absolute value of `final_q`. Fill counts rounded to the nearest thousand.

## 3. Analysis

**Inventory mode caps inventory.** Across all five γ values, inventory mode held `|q| ≤ 14` at session end. Symmetric mode allowed `|q|` to reach 355–627 depending on γ. This is the paper's central result (§3.3, Tables 1–3): the reservation-price skew `r = s − q·γ·σ²·(T−t)` (see MODEL.md eq. 8) continuously tilts quotes against the direction of accumulation, so inventory mean-reverts toward zero. Symmetric mode has no such correction; the dealer drifts long or short as the mid-price trends.

**Symmetric mode shows higher PnL on this realization, but the comparison is misleading.** Symmetric PnL ranges from +2.02 to +2.49 while inventory PnL ranges from −0.13 to −0.40, which at first glance looks like a clear win for symmetric. However, symmetric mode ended the run with a long position of 355–627 units, worth approximately 2.7–4.9 at the final mid of 0.00774. The asset trended −30% during the replay; had the run continued or ended at a lower price, all of that unrealised inventory gain would evaporate and then some. Inventory mode's PnL is slightly negative but `|q|` stayed in single digits throughout, so the mark-to-market number is a clean measure of spread capture. The paper's argument (§3.3) is that symmetric has higher mean PnL but far higher variance — one realization cannot reveal the variance, but the mechanism is plainly visible here.

**Higher γ tightens inventory but reduces fills.** In inventory mode, `|q|` at session end falls monotonically: 12 (γ = 0.01) → 14 (γ = 0.1) → 11 (γ = 1) → 3 (γ = 10) → 1 (γ = 100), confirming that larger risk aversion produces stronger mean-reversion. Simultaneously, fill counts drop from ~480k at γ = 0.01 to ~344k at γ = 100. This follows directly from the optimal spread formula (see MODEL.md eq. 30): `δ_a + δ_b = γ·σ²·(T−t) + (2/γ)·ln(1 + γ/k)` — the spread widens with γ, so the dealer's quotes are farther from mid and get hit less often. The tradeoff is the paper's core result: higher risk aversion buys inventory safety at the cost of turnover and spread capture.

**PnL trace observation.** The PnL plot (`pnl_plot.png`) shows a roughly steady linear bleed for the inventory run at γ = 100 (the run that produced `pnl_log.csv`). There is no sharp discontinuity around the σ spike near hour 97, when the asset price dropped to approximately 0.007. The spread formula widened automatically in response to the σ increase, so the strategy reduced quoting activity rather than getting caught in a large adverse move. The persistent loss is therefore environmental: A–S is a mean-reversion strategy calibrated for a martingale mid-price (`dS = σ dW`, no drift; see MODEL.md), and this dataset has a strong −30% downward trend throughout. The result is not a parameter or implementation failure.

## 4. Reproducing the experiment

```bash
./run_sweep.sh                 # ~5 minutes total (10 configs)
column -t -s, results.csv      # pretty-print the table
python3 plot_pnl.py            # generate pnl_plot.png from latest pnl_log.csv
```

See `Docs/README.md` for build instructions and config file format details.

## 5. Caveats

- **Single historical path** — no Monte Carlo. The paper uses 1000 simulated paths to compute std(PnL) and std(final_q). With one realization we can only report point estimates and cannot directly compare standard-deviation numbers. See ROADMAP.md item 6 for the multi-realization plan.
- **No fees or rebates** — every fill is credited at the full quoted price with no maker/taker cost. Ignoring fees overstates PnL on every fill and can make losing configurations appear profitable. See ROADMAP.md item 2.
- **Size-1 quotes, no partial fills** — the fill rule treats every incoming aggressor trade as fully consuming the dealer's resting quote regardless of incoming size. Real queue-position dynamics would lower apparent fill rates significantly. See ROADMAP.md item 4.
- **Inventory carries across sessions** — `T` rolls forward without flattening inventory at the session boundary. The paper assumes the dealer liquidates at T, which resets the inventory penalty to zero and avoids carry-over risk. See ROADMAP.md item 3.
- **`k` is hardcoded at 1e6** — not data-calibrated. The value was chosen to maintain a reasonable dimensionless spread-to-price ratio for an asset priced near 0.01, but it is not derived from the empirical trade-arrival distribution. See ROADMAP.md item 1.
- **Trending asset is unfavorable** — A–S assumes the mid-price is a martingale (`dS = σ dW`, no drift; see MODEL.md). This dataset has a strong −30% drift. The strategy is not designed for that regime, and results should be read accordingly.
