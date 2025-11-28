"""
Plot analysis for pre-copy results.

Reads `results/metrics_precopy.csv` (or a CSV path passed with `--csv`) and
plots state_size_bytes on the x-axis against:
 - latency_avg_pre_ms, latency_avg_during_ms, latency_avg_post_ms (combined)
 - state_inconsistency
 - migration_time_s
 - downtime_s
 - packet_loss_during_migration_pct

Saves an output SVG in `results/metrics_precopy_analysis.svg` by default.

Usage:
    python scripts/plot_precopy.py [--csv path/to/metrics_precopy.csv] [--out path/to/out.svg]

This script uses only the Python standard library and matplotlib.
"""
from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import List, Dict, Any

import matplotlib.pyplot as plt


DEFAULT_CSV = Path(__file__).resolve().parents[1] / "results" / "metrics_precopy.csv"
DEFAULT_OUT = Path(__file__).resolve().parents[1] / "results" / "metrics_precopy_analysis.svg"


def read_metrics(csv_path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")
    with csv_path.open("r", encoding="utf-8", newline="") as f:
        dr = csv.DictReader(f)
        for r in dr:
            rows.append(r)
    return rows


def to_float(v: str, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--csv", type=Path, default=DEFAULT_CSV, help="Path to metrics CSV (per-strategy)")
    p.add_argument("--out", type=Path, default=DEFAULT_OUT, help="Path to output SVG")
    p.add_argument(
        "--cluster-pct",
        type=float,
        default=50.0,
        help="Cluster state sizes that are within this percentage (default: 10.0)",
    )
    args = p.parse_args()

    data = read_metrics(args.csv)
    if not data:
        print("No rows found in CSV")
        return

    # Build mapping size -> list of rows
    rows_by_size: dict[int, list[Dict[str, Any]]] = {}
    for r in data:
        size = int(to_float(r.get("state_size_bytes", "0"), 0))
        rows_by_size.setdefault(size, []).append(r)

    if not rows_by_size:
        print("No numeric data to plot")
        return

    unique_sizes = sorted(rows_by_size.keys())

    # Cluster adjacent unique sizes using a running mean criterion driven by --cluster-pct
    cluster_pct = max(0.0, args.cluster_pct)
    clusters: list[list[int]] = []
    for s in unique_sizes:
        if not clusters:
            clusters.append([s])
            continue
        cur = clusters[-1]
        mean_cur = sum(cur) / len(cur)
        rel = abs(s - mean_cur) / max(1.0, mean_cur)
        if rel <= (cluster_pct / 100.0):
            cur.append(s)
        else:
            clusters.append([s])

    # For each cluster, collect all rows whose sizes are in the cluster and average metrics
    sizes = []
    lat_pre = []
    lat_during = []
    lat_post = []
    inconsistency = []
    migration_time = []
    downtime = []
    loss_pct = []

    for cl in clusters:
        rep_size = int(round(sum(cl) / len(cl)))
        sizes.append(rep_size)
        rows = []
        for s in cl:
            rows.extend(rows_by_size.get(s, []))

        def avg(field: str) -> float:
            vals = [to_float(r.get(field, "0"), 0.0) for r in rows]
            # ignore zero values when computing averages
            vals = [v for v in vals if v != 0.0]
            return sum(vals) / len(vals) if vals else 0.0

        lat_pre.append(avg("latency_avg_pre_ms"))
        lat_during.append(avg("latency_avg_during_ms"))
        lat_post.append(avg("latency_avg_post_ms"))
        inconsistency.append(avg("state_inconsistency"))
        migration_time.append(avg("migration_time_s"))
        downtime.append(avg("downtime_s"))
        loss_pct.append(avg("packet_loss_during_migration_pct"))

    # Decide whether to use log x-scale based on spread
    use_log_x = False
    if sizes and max(sizes) / max(1, min([s for s in sizes if s > 0] or [1])) > 8:
        use_log_x = True

    fig, axes = plt.subplots(2, 3, figsize=(14, 8))
    axes = axes.flatten()

    # 1) Latencies combined
    ax = axes[0]
    ax.scatter(sizes, lat_pre, label="latency_pre_ms", marker="o")
    ax.plot(sizes, lat_pre, linestyle="--", alpha=0.6)
    ax.scatter(sizes, lat_during, label="latency_during_ms", marker="s")
    ax.plot(sizes, lat_during, linestyle="--", alpha=0.6)
    ax.scatter(sizes, lat_post, label="latency_post_ms", marker="^")
    ax.plot(sizes, lat_post, linestyle="--", alpha=0.6)
    ax.set_xlabel("state_size_bytes")
    ax.set_ylabel("latency (ms)")
    ax.set_title("Latencies vs State Size")
    ax.grid(True)
    ax.legend()

    # 2) Migration time
    ax = axes[1]
    ax.scatter(sizes, migration_time)
    ax.plot(sizes, migration_time, linestyle="-", alpha=0.6)
    ax.set_xlabel("state_size_bytes")
    ax.set_ylabel("migration_time_s")
    ax.set_title("Migration Time vs State Size")
    ax.grid(True)

    # 3) Downtime
    ax = axes[2]
    ax.scatter(sizes, downtime)
    ax.plot(sizes, downtime, linestyle="-", alpha=0.6)
    ax.set_xlabel("state_size_bytes")
    ax.set_ylabel("downtime_s")
    ax.set_title("Downtime vs State Size")
    ax.grid(True)

    # 4) Packet loss
    ax = axes[3]
    ax.scatter(sizes, loss_pct)
    ax.plot(sizes, loss_pct, linestyle="-", alpha=0.6)
    ax.set_xlabel("state_size_bytes")
    ax.set_ylabel("packet_loss_during_migration_pct")
    ax.set_title("Packet Loss vs State Size")
    ax.grid(True)

    # Turn off remaining subplots
    for ax in axes[4:]:
        ax.axis("off")

    if use_log_x:
        for a in axes[:5]:
            a.set_xscale("log")

    plt.tight_layout()
    args.out.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(args.out, dpi=150)
    plt.close(fig)

    print(f"Saved plot to {args.out}")


if __name__ == "__main__":
    main()
