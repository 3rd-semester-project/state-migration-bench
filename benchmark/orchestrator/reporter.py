from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
from typing import List

import matplotlib.pyplot as plt  # type: ignore

from .config_loader import Config
from .metrics_collector import Metrics
from .utils import write_csv


class Reporter:
    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg
        self.results_dir = Path(cfg.general.results_dir)

    def save_metrics_csv(self, m: Metrics) -> Path:
        out = self.results_dir / f"metrics_{self.cfg.general.run_id}.csv"
        write_csv(out, [asdict(m)], list(asdict(m).keys()))
        return out

        
