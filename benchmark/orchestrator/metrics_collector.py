from __future__ import annotations

import io
import re
from dataclasses import dataclass
from statistics import mean
from typing import Iterable, Dict, Any, List

from docker.models.containers import Container  # type: ignore

from .config_loader import Config
from .utils import MigrationWindow

@dataclass
class Metrics:
    # Overall metrics
    run_id: str
    strategy: str

    # latency metrics (ms)
    migration_time_ms: float
    client_downtime_ms: float
    latency_before_downtime_ms: float

    # packet metrics
    packet_loss_during_migration_pct: int
    total_packets_successful: int
    total_packets: int

    # state metrics
    state_size_bytes: int


class MetricsCollector:
    CSV_PREFIX = "CSV:"

    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg

    def _parse_client_logs(self, containers: list[Container]) -> list[Dict[str, Any]]:
        # Each client prints: CSV: seq,send_ts,recv_ts,rtt_ms,status
        rows: list[Dict[str, Any]] = []
        for c in containers:
            raw: bytes = c.logs(stdout=True, stderr=False)
            for line in raw.decode("utf-8", errors="ignore").splitlines():
                if not line.startswith(self.CSV_PREFIX):
                    continue
                try:
                    _, payload = line.split(self.CSV_PREFIX, 1)
                    seq, send_ts, recv_ts, rtt_ms, status = payload.strip().split(",")
                    rows.append(
                        {
                            "client": c.name,
                            "seq": int(seq),
                            "send_ts": float(send_ts),
                            "recv_ts": float(recv_ts) if recv_ts else None,
                            "rtt_ms": float(rtt_ms) if rtt_ms else None,
                            "status": status,
                        }
                    )
                except Exception:
                    continue
        return rows

    def _window_slices(self, rows: list[Dict[str, Any]], win: MigrationWindow) -> tuple[list[Dict[str, Any]], list[Dict[str, Any]], list[Dict[str, Any]]]:
        pre = [r for r in rows if r["send_ts"] < win.start_ts]
        during = [r for r in rows if win.start_ts <= r["send_ts"] <= win.end_ts]
        post = [r for r in rows if r["send_ts"] > win.end_ts]
        return pre, during, post

    # Compute downtime as the time between last ok before migration and first ok after migration
    def _compute_downtime(self, rows: list[Dict[str, Any]], win: MigrationWindow) -> float:
        before = [r for r in rows if r["send_ts"] < win.start_ts and r["status"] == "ok"]
        after = [r for r in rows if r["send_ts"] > win.end_ts and r["status"] == "ok"]
        last_ok_ts = max([r["send_ts"] for r in before], default=win.start_ts)
        first_ok_ts = min([r["send_ts"] for r in after], default=win.end_ts)
        downtime = max(0.0, first_ok_ts - last_ok_ts)
        return downtime

    # Compute latency just between the last ok before migration and migration start
    def _compute_latency_before(self, rows: list[Dict[str, Any]], win: MigrationWindow) -> float:
        before = [r for r in rows if r["send_ts"] < win.start_ts and r["status"] == "ok"]
        last_ok_ts = max([r["send_ts"] for r in before], default=win.start_ts)
        latency_before = max(0.0, last_ok_ts - win.start_ts)
        return latency_before

    def _compute_packet_metrics(
        self, rows: list[Dict[str, Any]], win: MigrationWindow
    ) -> tuple[int, int, int]:

        during = [r for r in rows if win.start_ts <= r["send_ts"] <= win.end_ts]
        during_total = len(during)
        during_ok = sum(1 for r in during if r.get("status") == "ok")
        during_lost = max(0, during_total - during_ok)

        if during_total > 0:
            packet_loss_pct = int(round((during_lost / during_total) * 100))
        else:
            packet_loss_pct = 0

        total_packets = len(rows)
        total_packets_successful = sum(1 for r in rows if r.get("status") == "ok")

        return packet_loss_pct, total_packets_successful, total_packets

    def collect(
        self,
        containers: list[Container],
        win: MigrationWindow,
        state_diff: int,
        strategy: str,
    ) -> Metrics:
        rows = self._parse_client_logs(containers)

        downtime_s = self._compute_downtime(rows, win)
        latency_before_s = self._compute_latency_before(rows, win)

        client_downtime_ms = downtime_s * 1000.0
        latency_before_downtime_ms = latency_before_s * 1000.0
        migration_time_ms = client_downtime_ms + latency_before_downtime_ms
        (
            packet_loss,
            tot_packets_successful,
            tot_packets,
        ) = self._compute_packet_metrics(rows, win)

        print("rows parsed list:", rows)
        return Metrics(
            run_id=self.cfg.general.run_id,
            strategy=strategy,
            migration_time_ms=migration_time_ms,
            client_downtime_ms=client_downtime_ms,
            latency_before_downtime_ms=latency_before_downtime_ms,

            packet_loss_during_migration_pct=packet_loss,
            total_packets_successful=tot_packets_successful,
            total_packets=tot_packets,

            state_size_bytes=state_diff,
        )
