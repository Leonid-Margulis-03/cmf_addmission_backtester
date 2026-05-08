# README.md — Avellaneda–Stoikov Backtester

## Project layout

```
main.cpp              — backtester + A-S strategy; single translation unit
csv_readers.hpp       — fast CSV parsers for LOB and trade feeds
MD/lob.csv            — historical limit order book snapshots (25 levels)
MD/trades.csv         — historical aggressor trades
configs/example.cfg   — annotated config with all keys and default values
configs/g*_inv.cfg    — sweep configs: inventory mode, five gamma values
configs/g*_sym.cfg    — sweep configs: symmetric mode, five gamma values
pnl_logs/             — per-run PnL trace CSVs written by the sweep
results.csv           — aggregated sweep output (one row per config)
run_sweep.sh          — bash script that generates configs and runs all 10
plot_pnl.py           — plots pnl_log.csv to pnl_plot.png
build_command.txt     — canonical build flags
Docs/MODEL.md              — strategy math, code mapping, and extensions
Docs/README.md             — this file
Docs/ROADMAP.md            — improvement roadmap
```

## Build

```bash
g++ -std=c++17 -O3 -DNDEBUG main.cpp -o main
```

(The `build_command.txt` also lists a variant with `-pthread -I/opt/homebrew/include`; that line is for macOS Homebrew environments. The line above is sufficient on any standard platform.)

## Run a single config

```bash
./main configs/example.cfg
```

Prints per-10 000-update progress to stdout, writes PnL trace to `pnl_log.csv`, and appends one summary row to `results.csv`.

## Run the full sweep

```bash
./run_sweep.sh
```

Generates 10 config files under `configs/` (five gamma values × two modes), clears `results.csv`, then runs each config in sequence. Each run takes roughly 10 minutes on this dataset; total wall time is approximately 100 minutes. Output:
- `results.csv` — one row per config.
- `pnl_logs/g<gamma>_<mode>.csv` — per-run PnL traces.

## Plot results

```bash
python3 plot_pnl.py
```

Reads `pnl_log.csv` (the most recent single-config run) and writes `pnl_plot.png`. Requires `pandas` and `matplotlib`.

## Data format

### LOB CSV (`MD/lob.csv`)

Header:

```
,local_timestamp,asks[0].price,asks[0].amount,bids[0].price,bids[0].amount,...
```

- First column is an unnamed integer index; skipped by the reader.
- `local_timestamp` is microseconds since Unix epoch.
- 25 ask levels follow, sorted ascending by price (best ask first).
- 25 bid levels follow, sorted descending by price (best bid first).
- Column pattern repeats for all 25 levels.

The C++ struct is defined at `csv_readers.hpp:11–17`:

```cpp
struct BookSnapshot {
    uint64_t local_timestamp;
    std::array<double, 25> ask_price;
    std::array<double, 25> ask_amount;
    std::array<double, 25> bid_price;
    std::array<double, 25> bid_amount;
};
```

### Trades CSV (`MD/trades.csv`)

Header:

```
,local_timestamp,side,price,amount
```

- `side` is the aggressor direction: literal string `"buy"` or `"sell"`.
- The reader maps `"buy"` to `'B'` and `"sell"` to `'S'` (see `csv_readers.hpp:19–24`).

```cpp
struct Trade {
    uint64_t local_timestamp;
    char side;    // 'B' for "buy", 'S' for "sell"
    double price;
    double amount;
};
```

## Config file format

Format: `key=value`, one per line. Lines starting with `#` and blank lines are ignored.

| Key | Type | Default | Description |
|---|---|---|---|
| `config_id` | string | `default` | Identifier written to results.csv |
| `gamma` | float | `0.1` | A-S risk-aversion γ |
| `k` | float | `1e6` | Order-arrival intensity decay constant |
| `horizon_seconds` | float | `3600` | Session length T (seconds) |
| `vol_window` | int | `2000` | Rolling window size for σ estimation (samples) |
| `lob_path` | string | `MD/lob.csv` | Path to LOB CSV |
| `trades_path` | string | `MD/trades.csv` | Path to trades CSV |
| `pnl_log_path` | string | `pnl_log.csv` | Per-run PnL trace output |
| `results_csv` | string | `results.csv` | Aggregated sweep output (append mode) |
| `use_inventory_skew` | bool | `true` | `true` for A-S inventory mode; `false` for symmetric benchmark |

See `MODEL.md` for the mathematical role of each parameter.

## Output files

### `results.csv`

One row per completed run. Columns:

```
config_id, gamma, use_inventory_skew, book_updates, total_fills,
buys, sells, final_q, final_x, final_mid, turnover, pnl
```

- `final_x` — cash position at end of replay.
- `final_q` — inventory (signed) at end of replay.
- `pnl` — `final_x + final_q * final_mid` (mark-to-market).

### `pnl_logs/<id>.csv`

Sampled every 1000 book updates. Columns:

```
t, mid, q, x, pnl, sigma, spread
```

- `t` — wall-clock time in seconds.
- `sigma` — current σ estimate.
- `spread` — current optimal spread (0 before vol warms up).
