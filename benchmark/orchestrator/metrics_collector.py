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
    run_id: str
    strategy: str
    migration_time_s: float
    downtime_s: float
    packet_loss_during_migration_pct: float
    latency_avg_pre_ms: float
    latency_avg_during_ms: float
    latency_avg_post_ms: float
    state_inconsistency: int
    state_size_bytes: int = 0


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

    def _compute_downtime(self, rows: list[Dict[str, Any]], win: MigrationWindow) -> float:
        # Downtime = longest continuous window around [start,end] where status != ok
        # Approximation: time between last success before switch and first success after switch
        before = [r for r in rows if r["send_ts"] < win.start_ts and r["status"] == "ok"]
        after = [r for r in rows if r["send_ts"] > win.end_ts and r["status"] == "ok"]
        last_ok_ts = max([r["send_ts"] for r in before], default=win.start_ts)
        first_ok_ts = min([r["send_ts"] for r in after], default=win.end_ts)
        downtime = max(0.0, first_ok_ts - last_ok_ts)
        return downtime

    def collect(
        self,
        containers: list[Container],
        win: MigrationWindow,
        state_diff: int,
        strategy: str,
        state_size_bytes: int = 0,
    ) -> Metrics:
        rows = self._parse_client_logs(containers)
        pre, during, post = self._window_slices(rows, win)

        def ok_latencies(rs: list[Dict[str, Any]]) -> list[float]:
            return [r["rtt_ms"] for r in rs if r["status"] == "ok" and r["rtt_ms"] is not None]

        latency_pre = mean(ok_latencies(pre)) if ok_latencies(pre) else 0.0
        latency_during = mean(ok_latencies(during)) if ok_latencies(during) else 0.0
        latency_post = mean(ok_latencies(post)) if ok_latencies(post) else 0.0

        # Packet loss during migration window only
        total_during = len(during)
        lost_during = len([r for r in during if r["status"] != "ok"])
        loss_pct = (lost_during / total_during * 100.0) if total_during > 0 else 0.0

        downtime = self._compute_downtime(rows, win)
        return Metrics(
            run_id=self.cfg.general.run_id,
            strategy=strategy,
            migration_time_s=max(0.0, win.end_ts - win.start_ts),
            downtime_s=downtime,
            packet_loss_during_migration_pct=loss_pct,
            latency_avg_pre_ms=latency_pre,
            latency_avg_during_ms=latency_during,
            latency_avg_post_ms=latency_post,
            state_inconsistency=state_diff,
            state_size_bytes=state_size_bytes,
        )
