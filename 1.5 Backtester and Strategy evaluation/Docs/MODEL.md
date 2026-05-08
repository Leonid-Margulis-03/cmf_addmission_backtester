# MODEL.md — Avellaneda–Stoikov Market-Making Strategy

## The Avellaneda–Stoikov model

Avellaneda and Stoikov (2008) model a dealer who posts bid and ask limit orders continuously and maximizes expected exponential utility of terminal wealth. The dealer's problem leads to two central results. First, the dealer quotes around a *reservation price* that adjusts the mid-price by an inventory-penalty term (paper eq. 8):

```
r = s − q · γ · σ² · (T − t)
```

Second, the total spread around that reservation price is set to maximize expected fill rate (paper eq. 30):

```
δ_a + δ_b = γ · σ² · (T − t) + (2/γ) · ln(1 + γ/k)
```

Parameter glossary:
- `s` — current mid-price (fair value reference).
- `q` — current inventory (signed; positive = long).
- `γ` — risk-aversion coefficient; larger γ produces wider spreads and stronger inventory skew.
- `σ` — volatility of the mid-price (units: price / sqrt(second)).
- `T − t` — time remaining in the session horizon.
- `k` — order-arrival intensity decay; controls how quickly fill probability falls as the quote moves away from mid.

## Code mapping

| Paper concept | Code symbol | File:line |
|---|---|---|
| Reservation price (eq. 8) | `ReservationPrice()` | main.cpp:231–234 |
| Optimal spread (eq. 30) | `OptimalSpread()` | main.cpp:236–239 |
| σ estimator | `VolatilityHandler` class | main.cpp:61–103 |
| Mid replacement (microprice) | `BookUpdate` body | main.cpp:121–122 |
| Fill rule | `TradeUpdate` | main.cpp:165–183 |
| Inventory-skew flag | `use_inventory_skew_` | main.cpp:259 |
| Session horizon (`T = t + horizon`) | `T = t + horizon_seconds_` | main.cpp:127 |
| Mid (`s`, microprice-weighted) | `s` set in `BookUpdate` | main.cpp:121–122 |

All line numbers confirmed by grep against the current `main.cpp`.

## Volatility estimator

`VolatilityHandler` computes realized variance per unit time over a rolling window of the last `N` mid-price updates. Each tick contributes `(Δs)²` and `Δt` to running sums; the estimator returns `σ = sqrt(Σ(Δs)² / Σ(Δt))`. This is time-weighted, so irregularly spaced ticks are handled correctly: a long quiet gap contributes a large `Δt` in the denominator, dampening the per-second variance estimate. The window size `N` defaults to 2000 samples and is configurable via `vol_window` in the config file.

## Microprice extension

The microprice weights the best bid and ask by the *opposite* side's queue size:

```
microprice = (bid · ask_size + ask · bid_size) / (bid_size + ask_size)
```

This gives an imbalance-aware fair value: if the ask queue is thin (aggressive buyers), the microprice leans toward the ask, reflecting short-term upward pressure. The simple mid (commented out at main.cpp:120) ignores this signal. In practice, top-of-book sizes on this asset are frequently balanced, so microprice is close to mid most of the time; the PnL impact of switching to microprice was small (~1%).

## Symmetric vs inventory mode

Both modes come from the A–S framework (paper §3.3). In *inventory mode* (`use_inventory_skew = true`), the reservation price is the full A–S result: `r = s − q·γ·σ²·(T−t)`. As inventory grows long, `r` falls below mid and the dealer's quotes shift down, making it cheaper to buy back inventory. In *symmetric mode* (`use_inventory_skew = false`), the reservation price is fixed at `r = s` with no inventory adjustment; the spread formula still applies but both quotes stay symmetric around mid regardless of position. The mode is selected at runtime via the `use_inventory_skew` config key (see `README.md` for config format).
