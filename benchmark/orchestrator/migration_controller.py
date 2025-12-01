from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Literal, Tuple, Dict, Any

import requests
from docker.models.containers import Container  # type: ignore

from .config_loader import Config
from .docker_manager import DockerManager
from .utils import now_ts, MigrationWindow


@dataclass
class StateConsistency:
    pre_counter: int
    post_counter: int
    diff: int
    state_size_bytes: int


class MigrationController:
    def __init__(self, cfg: Config, dm: DockerManager) -> None:
        self.cfg = cfg
        self.dm = dm
        #self.port = cfg.servers.port
        self.host_ports: dict[str, int]= {} # service_alias => host port

    def register_host_ports(self, server_a: Container, server_b: Container):
        self.host_ports[server_a.name] = self.cfg.servers.port #5000
        self.host_ports[server_b.name] = self.cfg.servers.port + 1 #5001


    # HTTP helpers

    def _url(self, container: Container, path: str) -> str:
        host_port = self.host_ports[container.name]
        return f"http://localhost:{host_port}{path}"

    def _get_state(self, c: Container) -> Dict[str, Any]:
        resp = requests.get(self._url(c, "/state"), timeout=30)
        resp.raise_for_status()
        return resp.json()

    def _import_state(self, c: Container, state: Dict[str, Any]) -> None:
        resp = requests.post(self._url(c, "/state"), json=state, timeout=30)
        resp.raise_for_status()

    # Strategies

    def run(self, server_a: Container, server_b: Container) -> Tuple[MigrationWindow, MigrationWindow, MigrationWindow, StateConsistency]:
        # Allow initial traffic
        time.sleep(self.cfg.migration.delay_s)

        if self.cfg.migration.strategy == "precopy":
            return self._run_precopy(server_a, server_b)
        if self.cfg.migration.strategy == "postcopy":
            return self._run_postcopy(server_a, server_b)
        raise ValueError(f"Unknown migration strategy: {self.cfg.migration.strategy}")

    def _run_precopy(self, server_a: Container, server_b: Container) -> Tuple[MigrationWindow, MigrationWindow, MigrationWindow, StateConsistency]:
        # Total migration start: before initial pre-copy
        total_start = now_ts()

        # Initial pre-transfer while clients still connected to A
        initial_start = now_ts()
        print("[precopy] starting initial pre-transfer at", initial_start-total_start)
        initial_state = self._get_state(server_a)
        # mark where initial migration happened (log the counter)
        try:
            marker = int(initial_state.get("counter", 0))
        except Exception:
            marker = None
        print(f"[precopy] initial pre-transfer marker counter={marker} at", now_ts()-total_start)
        # Prepare a partial state that only includes blobs up to marker
        initial_blob = {}
        raw_blob = initial_state.get("blob", {}) or {}
        if isinstance(raw_blob, dict) and marker is not None:
            for k, v in raw_blob.items():
                try:
                    if int(k) <= marker:
                        initial_blob[k] = v
                except Exception:
                    # ignore non-int keys
                    continue
        # Build minimal import payload
        initial_import = {
            "counter": marker or int(initial_state.get("counter", 0)),
            "last_seq": int(initial_state.get("last_seq", -1)),
            "blob": initial_blob,
        }
        self._import_state(server_b, initial_import)
        initial_end = now_ts()
        print("[precopy] completed initial pre-transfer at", initial_end-total_start)

        initial_window = MigrationWindow(initial_start, initial_end)

        # Now cut the client connection (clients will experience downtime)
        downtime_start = now_ts()
        print("[precopy] starting downtime at", downtime_start-total_start)
        self.dm.drop_alias(server_a)

        # Transfer the remaining state after cut
        final_state = self._get_state(server_a)
        # Prepare remaining blobs (those with counter > marker)
        remaining_blob = {}
        raw_final_blob = final_state.get("blob", {}) or {}
        if isinstance(raw_final_blob, dict) and marker is not None:
            for k, v in raw_final_blob.items():
                try:
                    if int(k) > marker:
                        remaining_blob[k] = v
                except Exception:
                    continue
        final_import = {
            "counter": int(final_state.get("counter", 0)),
            "last_seq": int(final_state.get("last_seq", -1)),
            "blob": remaining_blob,
        }
        self._import_state(server_b, final_import)
        print("[precopy] completed final transfer at", now_ts()-total_start)

        # Reconnect clients to B
        self.dm.attach_alias(server_b)
        downtime_end = now_ts()
        print("[precopy] ended downtime at", downtime_end-total_start)

        # Post consistency check
        post_state = self._get_state(server_b)
        print("[precopy] completed post consistency check at", now_ts()-total_start)
        total_end = downtime_end
        # Return (total_window, downtime_window, initial_window, consistency)
        return (
            MigrationWindow(total_start, total_end),
            MigrationWindow(downtime_start, downtime_end),
            initial_window,
            self._consistency(initial_state, post_state),
        )

    def _run_postcopy(self, server_a: Container, server_b: Container) -> Tuple[MigrationWindow, MigrationWindow, MigrationWindow, StateConsistency]:
        # Postcopy: disconnect clients first, then transfer, then reconnect
        total_start = now_ts()

        # Disconnect clients (start downtime)
        downtime_start = now_ts()
        self.dm.drop_alias(server_a)
        print("[postcopy] clients disconnected at", downtime_start - total_start)

        # Transfer full state from A to B while clients disconnected
        pre_state = self._get_state(server_a)
        self._import_state(server_b, pre_state)
        print("[postcopy] state transfer completed at", now_ts() - total_start)

        # Reconnect clients to B
        self.dm.attach_alias(server_b)
        downtime_end = now_ts()
        print("[postcopy] clients reconnected at", downtime_end - total_start)

        post_state = self._get_state(server_b)
        total_end = downtime_end
        print("[postcopy] completed post consistency check at", now_ts() - total_start)

        # For postcopy there was no initial background copy; return an empty initial window
        initial_window = MigrationWindow(total_start, total_start)
        return (
            MigrationWindow(total_start, total_end),
            MigrationWindow(downtime_start, downtime_end),
            initial_window,
            self._consistency(pre_state, post_state),
        )

    def _consistency(self, pre: Dict[str, Any], post: Dict[str, Any]) -> StateConsistency:
        pre_counter = int(pre.get("counter", 0))
        post_counter = int(post.get("counter", 0))
        # Compute blob size on post state as the size of the transferred state.
        # New state format: blob is a dict of counter->blob_value. Sum lengths of values.
        def total_blob_bytes(b):
            if isinstance(b, dict):
                try:
                    return sum(len(v or "") for v in b.values())
                except Exception:
                    return 0
            if b is None:
                return 0
            try:
                return len(b)
            except Exception:
                return 0

        pre_blob = pre.get("blob", {})
        post_blob = post.get("blob", {})
        state_bytes = total_blob_bytes(post_blob)
        return StateConsistency(
            pre_counter=pre_counter,
            post_counter=post_counter,
            diff=abs(pre_counter - post_counter),
            state_size_bytes=state_bytes,
        )
