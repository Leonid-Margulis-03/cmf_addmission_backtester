import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv("pnl_log.csv")
t0 = df["t"].iloc[0]
df["t_hours"] = (df["t"] - t0) / 3600.0

fig, axes = plt.subplots(3, 1, figsize=(12, 9), sharex=True)
fig.suptitle("Avellaneda-Stoikov backtest")

# Top: PnL
axes[0].plot(df["t_hours"], df["pnl"], color="tab:blue", linewidth=0.8)
axes[0].set_ylabel("PnL")
axes[0].grid(True, alpha=0.3)

# Middle: mid (left) + q (right)
ax_mid = axes[1]
ax_q = ax_mid.twinx()
ax_mid.plot(df["t_hours"], df["mid"], color="tab:orange", linewidth=0.8, label="mid")
ax_q.plot(df["t_hours"], df["q"], color="tab:green", linewidth=0.8, label="q")
ax_mid.set_ylabel("mid", color="tab:orange")
ax_q.set_ylabel("q", color="tab:green")
ax_mid.tick_params(axis="y", labelcolor="tab:orange")
ax_q.tick_params(axis="y", labelcolor="tab:green")
ax_mid.grid(True, alpha=0.3)

# Bottom: sigma (left) + spread (right)
ax_sig = axes[2]
ax_sp = ax_sig.twinx()
ax_sig.plot(df["t_hours"], df["sigma"], color="tab:red", linewidth=0.8, label="sigma")
ax_sp.plot(df["t_hours"], df["spread"], color="tab:purple", linewidth=0.8, label="spread")
ax_sig.set_ylabel("sigma", color="tab:red")
ax_sp.set_ylabel("spread", color="tab:purple")
ax_sig.tick_params(axis="y", labelcolor="tab:red")
ax_sp.tick_params(axis="y", labelcolor="tab:purple")
ax_sig.set_xlabel("elapsed hours")
ax_sig.grid(True, alpha=0.3)

plt.tight_layout()
fig.savefig("pnl_plot.png", dpi=120, bbox_inches="tight")
