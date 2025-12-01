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

    def _validate_coverage(self, pre: list[Dict[str, Any]], during: list[Dict[str, Any]], post: list[Dict[str, Any]]) -> None:
        # Basic coherence checks: presence and status fields
        def has_status(rs: list[Dict[str, Any]]) -> bool:
            return all(("status" in r) for r in rs)
        if not has_status(pre) or not has_status(during) or not has_status(post):
            print("warn: missing status in some rows")
        # Sequence monotonicity per client (optional but useful)
        by_client: Dict[str, list[int]] = {}
        for r in pre + during + post:
            by_client.setdefault(r["client"], []).append(r["seq"])
        for client, seqs in by_client.items():
            if sorted(seqs) != seqs:
                print(f"warn: non-monotonic seq for {client}")

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
        latency_before = max(0.0, win.start_ts - last_ok_ts)
        return latency_before

    def _compute_packet_metrics(
            self, rows: list[Dict[str, Any]], win: MigrationWindow
        ) -> tuple[float, int, int, Dict[str, int]]:
            pre, during, post = self._window_slices(rows, win)
            self._validate_coverage(pre, during, post)

            during_total = len(during)
            during_ok = sum(1 for r in during if r.get("status") == "ok")
            during_lost = max(0, during_total - during_ok)
            loss_pct_during = (during_lost / during_total * 100.0) if during_total > 0 else 0.0

            post_total = len(post)
            post_ok = sum(1 for r in post if r.get("status") == "ok")
            post_lost = max(0, post_total - post_ok)
            loss_pct_post = (post_lost / post_total * 100.0) if post_total > 0 else 0.0

            total_packets = len(rows)
            total_packets_successful = sum(1 for r in rows if r.get("status") == "ok")

            debug_counts = {
                "pre_total": len(pre),
                "pre_ok": sum(1 for r in pre if r.get("status") == "ok"),
                "during_total": during_total,
                "during_ok": during_ok,
                "post_total": len(post),
                "post_ok": sum(1 for r in post if r.get("status") == "ok"),
            }
            return (during_lost + post_lost), total_packets_successful, total_packets, debug_counts

    def collect(
        self,
        containers: list[Container],
        win: MigrationWindow,
        state_diff: int,
        strategy: str,
    ) -> Metrics:
        rows = self._parse_client_logs(containers)

        latency_before_s = self._compute_latency_before(rows, win)
        downtime_s = self._compute_downtime(rows, win)

        client_downtime_ms = downtime_s * 1000.0
        latency_before_downtime_ms = latency_before_s * 1000.0
        migration_time_ms = client_downtime_ms + latency_before_downtime_ms

        loss_pct_during, tot_packets_successful, tot_packets, dbg = self._compute_packet_metrics(rows, win)
        print("window counts:", dbg)

        return Metrics(
            run_id=self.cfg.general.run_id,
            strategy=strategy,
            migration_time_ms=migration_time_ms,
            client_downtime_ms=client_downtime_ms,
            migration_time_before_ms=latency_before_downtime_ms,
            packet_loss_during_migration_pct=loss_pct_during,
            total_packets_successful=tot_packets_successful,
            total_packets=tot_packets,
            state_size_bytes=state_diff,
        )
