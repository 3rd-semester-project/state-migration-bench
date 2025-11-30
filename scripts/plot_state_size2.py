from pathlib import Path
import json

p = Path("results/metrics_state_size2.csv")
print("CSV path:", p.resolve())
if not p.exists():
    print("File not found:", p)
    raise SystemExit(1)

try:
    import pandas as pd
except Exception as e:
    print("pandas import failed:", e)
    print("Install pandas: python -m pip install pandas")
    raise

df = pd.read_csv(p)
print("DataFrame shape:", df.shape)
print("DataFrame columns:", df.columns.tolist())
print("First 5 rows:")
print(df.head())
import matplotlib.pyplot as plt  # type: ignore
import numpy as np
import matplotlib.ticker as mticker
from pathlib import Path


# Create output directory
out_dir = Path("results/plots")
out_dir.mkdir(parents=True, exist_ok=True)


def ensure_numeric(df, cols):
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def create_stats_table(df: "pd.DataFrame", strategy: str = "precopy", x_col: str = "delay_s") -> "pd.DataFrame":
    """Create a summary table grouped by `x_col` for the given strategy.

    The returned DataFrame contains the averaged values (mean) for the
    metrics shown on the plots for that strategy as well as packet loss,
    total packets and state size. It also includes a `runs` column with the
    number of samples averaged for each x value.

    Columns returned: x_col, migration_time_ms, client_downtime_ms,
    latency_before_downtime_ms, packet_loss_during_migration_pct,
    total_packets_successful, total_packets, state_size_bytes, runs
    """
    df2 = df.copy()

    # determine which config column to use (config_yaml or config_json)
    config_col = None
    if "config_yaml" in df2.columns:
        config_col = "config_yaml"
    elif "config_json" in df2.columns:
        config_col = "config_json"

    # ensure x_col exists (extract from config if needed)
    if x_col not in df2.columns or df2[x_col].isna().all():
        def _extract(s: str):
            try:
                cfg = json.loads(s)
                return cfg.get("migration", {}).get("delay_s")
            except Exception:
                return None

        if config_col:
            df2[x_col] = df2[config_col].apply(_extract)
    df2[x_col] = pd.to_numeric(df2[x_col], errors="coerce")

    # filter by strategy
    df2 = df2[df2["strategy"] == strategy]

    if df2.empty:
        return pd.DataFrame()

    metrics = [
        "migration_time_ms",
        "client_downtime_ms",
        "latency_before_downtime_ms",
        "packet_loss_during_migration_pct",
        "total_packets_successful",
        "total_packets",
        "state_size_bytes",
    ]

    # use named aggregation: output_col -> (input_col, aggfunc)
    # perform simple groupby.mean() and count
    present_metrics = [m for m in metrics if m in df2.columns]
    group = df2.groupby(x_col)
    if present_metrics:
        grouped = group[present_metrics].mean()
    else:
        grouped = pd.DataFrame(index=group.size().index)

    runs = group.size().rename("runs")
    grouped = grouped.join(runs)
    grouped = grouped.reset_index()
    return grouped

def multiplot_state_size(df: pd.DataFrame, out_path: Path):
    """Create a 2-panel multiplot comparing strategies.

    Left: precopy; Right: postcopy. Each panel contains three lines vs
    `state_size_bytes`: migration_time_ms, client_downtime_ms,
    latency_before_downtime_ms.
    """
    cols = [
        "migration_time_ms",
        "client_downtime_ms",
        "latency_before_downtime_ms",
        "state_size_bytes",
    ]
    df = ensure_numeric(df.copy(), cols)

    strategies = ["precopy", "postcopy"]
    fig, axes = plt.subplots(1, 2, figsize=(14, 6), sharey=True)

    colors = {
        "migration_time_ms": "C0",
        "client_downtime_ms": "C1",
        "latency_before_downtime_ms": "C2",
    }

    for ax, strat in zip(axes, strategies):
        sub = df[df["strategy"] == strat]
        if sub.empty:
            ax.text(0.5, 0.5, f"No data for {strat}", ha="center", va="center")
            ax.set_title(strat)
            continue

        # extract delay_s from config_yaml/config_json if not present or NaN
        if "delay_s" not in sub.columns or sub["delay_s"].isna().all():
            def _extract_delay(s: str):
                try:
                    cfg = json.loads(s)
                    return cfg.get("migration", {}).get("delay_s")
                except Exception:
                    return None

            sub = sub.copy()
            config_col = "config_yaml" if "config_yaml" in sub.columns else ("config_json" if "config_json" in sub.columns else None)
            if config_col:
                sub["delay_s"] = sub[config_col].apply(_extract_delay)
                sub["delay_s"] = pd.to_numeric(sub["delay_s"], errors="coerce")
            else:
                # no config column available; create empty delay_s
                sub["delay_s"] = pd.Series([None] * len(sub), index=sub.index)

        sub = sub.sort_values("delay_s")
        # aggregate duplicate delay_s by averaging metric values
        agg_metrics = [m for m in [
            "migration_time_ms",
            "client_downtime_ms",
            "latency_before_downtime_ms",
        ] if m in sub.columns]
        if agg_metrics:
            agg_df = sub.groupby("delay_s", as_index=False)[agg_metrics].mean()
        else:
            agg_df = sub[["delay_s"]].drop_duplicates().copy()
        x = agg_df["delay_s"].astype(float)

        # choose metrics: postcopy only shows client_downtime_ms, precopy shows all
        metrics_to_plot = ["client_downtime_ms"] if strat == "postcopy" else [
            "migration_time_ms",
            "client_downtime_ms",
            "latency_before_downtime_ms",
        ]

        for metric in metrics_to_plot:
            if metric in agg_df.columns:
                y = agg_df[metric].astype(float)
                ax.plot(x, y, marker="o", label=metric, color=colors.get(metric))

        ax.set_xlabel("delay_s")
        # format x tick labels as plain floats with one decimal
        try:
            import matplotlib.ticker as mticker
            fmt = mticker.FuncFormatter(lambda v, pos: f"{v:.1f}")
            ax.xaxis.set_major_formatter(fmt)
            ax.ticklabel_format(style='plain', axis='x')
        except Exception:
            pass
        ax.set_title(strat)
        ax.grid(True, which="both", ls="--", alpha=0.3)
        # draw vertical red line at delay_s = 5
        try:
            ax.axvline(5, color="red", linestyle="--", linewidth=1.5)
        except Exception:
            # if something goes wrong with the axvline call, continue without failing
            pass
        ax.legend()

    axes[0].set_ylabel("milliseconds")
    plt.suptitle("Migration metrics vs state size (precopy vs postcopy)")
    plt.tight_layout(rect=[0, 0.03, 1, 0.95])
    plt.savefig(out_path)
    plt.close(fig)


out_file = out_dir / "state_size2_multiplot.svg"
multiplot_state_size(df, out_file)
print(f"Saved multiplot to: {out_file.resolve()}")


# generate and save stats tables for precopy and postcopy
def _save_table_svg(table_df, filename: Path, title: str | None = None):
    import matplotlib.pyplot as _plt

    if table_df is None or table_df.empty:
        # create a small figure saying no data
        fig, ax = _plt.subplots(figsize=(6, 2))
        ax.text(0.5, 0.5, "No data", ha="center", va="center")
        ax.axis("off")
        fig.savefig(filename)
        _plt.close(fig)
        return

    def format_bytes(n):
        try:
            n = float(n)
        except Exception:
            return ""
        units = ["B", "KB", "MB", "GB", "TB"]
        i = 0
        while abs(n) >= 1024 and i < len(units) - 1:
            n /= 1024.0
            i += 1
        if i == 0:
            return f"{int(n):,}{units[i]}"
        return f"{n:,.1f}{units[i]}"

    # Prepare a display DataFrame with shorter column labels and formatted numbers
    df_disp = table_df.copy()
    # mapping to short names
    short_names = {
        "migration_time_ms": "mig_ms",
        "client_downtime_ms": "down_ms",
        "latency_before_downtime_ms": "lat_ms",
        "packet_loss_during_migration_pct": "loss_pct",
        "total_packets_successful": "succ_pkts",
        "total_packets": "total_pkts",
        "state_size_bytes": "state",
        "runs": "runs",
    }

    # apply formatting per column
    for c in df_disp.columns.tolist():
        if c in ["runs", "total_packets", "total_packets_successful"]:
            # integers
            df_disp[c] = df_disp[c].map(lambda v: f"{int(v):,}" if pd.notna(v) else "")
        elif c == "state_size_bytes":
            df_disp[c] = df_disp[c].map(lambda v: format_bytes(v) if pd.notna(v) else "")
        elif c in ["migration_time_ms", "client_downtime_ms", "latency_before_downtime_ms"]:
            df_disp[c] = df_disp[c].map(lambda v: f"{v:,.1f}" if pd.notna(v) else "")
        elif c == "packet_loss_during_migration_pct":
            df_disp[c] = df_disp[c].map(lambda v: f"{v:.1f}" if pd.notna(v) else "")
        else:
            df_disp[c] = df_disp[c].astype(str)

    # rename columns to short labels
    rename_map = {c: short_names.get(c, c) for c in df_disp.columns.tolist()}
    df_disp = df_disp.rename(columns=rename_map)

    cell_text = df_disp.values.tolist()
    col_labels = df_disp.columns.tolist()

    # size figure by number of rows and columns
    rows = max(1, len(cell_text))
    cols = max(1, len(col_labels))
    fig_h = max(2, 0.35 * rows)
    fig_w = max(6, 1.2 * cols)
    fig, ax = _plt.subplots(figsize=(fig_w, fig_h))
    ax.axis("off")
    if title:
        ax.set_title(title)

    table = ax.table(cellText=cell_text, colLabels=col_labels, cellLoc="center", loc="center")
    table.auto_set_font_size(False)
    # adjust fontsize to fit
    table.set_fontsize(max(6, min(10, int(200 / max(cols, rows)))))
    table.scale(1.2, 1.2)
    fig.tight_layout()
    fig.savefig(filename)
    _plt.close(fig)


for strat in ["precopy", "postcopy"]:
    tbl = create_stats_table(df, strategy=strat, x_col="delay_s")
    fname = out_dir / f"stats_state_size2_{strat}.svg"
    _save_table_svg(tbl, fname, title=f"Stats ({strat})")
    print(f"Saved stats table: {fname.resolve()}")
