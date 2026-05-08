#!/usr/bin/env bash
set -e

mkdir -p configs pnl_logs
rm -f results.csv

for gamma in 0.01 0.1 1 10 100; do
    for mode in inv sym; do
        cfg="configs/g${gamma}_${mode}.cfg"

        if [ "$mode" = "inv" ]; then
            skew=true
        else
            skew=false
        fi

        cat > "$cfg" <<EOF
config_id=g${gamma}_${mode}
gamma=${gamma}
k=1e6
horizon_seconds=3600
use_inventory_skew=${skew}
lob_path=MD/lob.csv
trades_path=MD/trades.csv
pnl_log_path=pnl_logs/g${gamma}_${mode}.csv
results_csv=results.csv
EOF

        echo "Running config: $cfg"
        ./main "$cfg"
    done
done
