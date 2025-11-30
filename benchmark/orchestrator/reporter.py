from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
from typing import List

import matplotlib.pyplot as plt  # type: ignore

from .config_loader import Config
from .metrics_collector import Metrics
from .utils import write_csv, update_csv


class Reporter:
    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg
        self.results_dir = Path(cfg.general.results_dir)

    def save_metrics_csv(self, m: Metrics) -> Path:
        out = self.results_dir / f"metrics_{self.cfg.general.run_id}.csv"
        update_csv(out, [asdict(m)], list(asdict(m).keys()))
        return out

    def plot_metrics(self, m: Metrics) -> Path:
        out_png = self.results_dir / f"metrics_{self.cfg.general.run_id}.png"
        out_png.parent.mkdir(parents=True, exist_ok=True)

        # Two subplots: time vs downtime+migration_time and latency bars
        fig, axes = plt.subplots(1, 2, figsize=(10, 4))
        fig.suptitle(f"Migration metrics [{m.strategy}] - run {m.run_id}")

        # Times
        axes[0].bar(["migration_time", "downtime", "latency_before_downtime"], [m.migration_time_ms, m.client_downtime_ms, m.latency_before_downtime_ms], color=["#4e79a7", "#f28e2c", "#59a14f"])
        axes[0].set_ylabel("milliseconds")

        # Loss
        axes[1].bar(
            ["Loss", "total"],
            [m.packet_loss_during_migration_pct/100 * m.total_packets, m.total_packets],
            color=["#59a14f", "#e15759"],
        )
        axes[1].set_ylabel("ms")
        axes[1].set_title(f"loss during: {m.packet_loss_during_migration_pct:.1f}% of {m.total_packets} packets")

        plt.tight_layout()
        plt.savefig(out_png, dpi=120)
        plt.close(fig)
        return out_png
