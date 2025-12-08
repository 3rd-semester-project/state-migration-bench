from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Literal, Tuple, Dict, Any, Optional

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

    def register_host_ports(self, server_a: Container, server_b: Optional[Container]):
        self.host_ports[server_a.name] = self.cfg.servers.port #5000
        if server_b is not None:
            self.host_ports[server_b.name] = self.cfg.servers.port + 1 #5001
        


    # HTTP helpers

    def _url(self, container: Container, path: str) -> str:
        host_port = self.host_ports[container.name]
        return f"http://localhost:{host_port}{path}"

    def _internal_url(self, container: Container) -> str:
        ip = self.dm.get_container_ip(container)
        return f"http://{ip}:{self.cfg.servers.port}"

    def _get_state_meta(self, c: Container) -> Dict[str, Any]:
        resp = requests.get(self._url(c, "/state_meta"), timeout=30)
        resp.raise_for_status()
        return resp.json()

    def _pull_state_remote(
        self,
        dest: Container,
        source_url: str,
        *,
        min_counter_exclusive: Optional[int] = None,
        max_counter_inclusive: Optional[int] = None,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {"source_url": source_url}
        if min_counter_exclusive is not None:
            payload["min_counter_exclusive"] = min_counter_exclusive
        if max_counter_inclusive is not None:
            payload["max_counter_inclusive"] = max_counter_inclusive
        resp = requests.post(self._url(dest, "/pull_state"), json=payload, timeout=60)
        resp.raise_for_status()
        return resp.json()

    # Strategies

    def run(self, server_a: Container, server_b: Container) -> Tuple[MigrationWindow, MigrationWindow, MigrationWindow, StateConsistency]:
        # Allow initial traffic
        time.sleep(self.cfg.migration.delay_s)


        if self.cfg.migration.strategy == "precopy":
            return self._run_precopy(server_a, server_b)
        if self.cfg.migration.strategy == "postcopy":
            return self._run_postcopy(server_a, server_b)
        raise ValueError(f"Unknown migration strategy: {self.cfg.migration.strategy}")

    def _run_precopy(self, server_a: Container, server_b: Optional[Container]) -> Tuple[MigrationWindow, MigrationWindow, MigrationWindow, StateConsistency]:
        # Total migration start: before initial pre-copy
        total_start = now_ts()

        if self.cfg.migration.dest_preboot == False:
            if server_b is None:
                server_b = self.dm.run_server_b() # run server B if not already running
                self.register_host_ports(server_a, server_b)
                print("[migration] destination pre-booted before migration")

        if server_b is None:
            raise ValueError("server_b is not available for precopy migration")

        source_internal_url = self._internal_url(server_a)

        # Initial pre-transfer while clients still connected to A
        initial_start = now_ts()
        print("[precopy] starting initial pre-transfer at", initial_start-total_start)
        initial_meta = self._get_state_meta(server_a)
        try:
            marker = int(initial_meta.get("counter", 0))
        except Exception:
            marker = None
        print(f"[precopy] initial pre-transfer marker counter={marker} at", now_ts()-total_start)
        max_counter = marker if marker is not None else None
        _ = self._pull_state_remote(
            server_b,
            source_internal_url,
            max_counter_inclusive=max_counter,
        )
        initial_end = now_ts()
        print("[precopy] completed initial pre-transfer at", initial_end-total_start)

        initial_window = MigrationWindow(initial_start, initial_end)

        # Now cut the client connection (clients will experience downtime)
        downtime_start = now_ts()
        print("[precopy] starting downtime at", downtime_start-total_start)
        self.dm.drop_alias(server_a)
        print("[precopy] clients disconnected at", now_ts()-total_start)
        # Transfer the remaining state after cut
        final_info = self._pull_state_remote(
            server_b,
            source_internal_url,
            min_counter_exclusive=marker,
        )
        print("[precopy] completed final transfer at", now_ts()-total_start)

        # Reconnect clients to B
        self.dm.attach_alias(server_b)
        downtime_end = now_ts()
        print("[precopy] ended downtime at", downtime_end-total_start)

        # Post consistency check
        print("[precopy] completed post consistency check at", now_ts()-total_start)
        total_end = downtime_end
        # Return (total_window, downtime_window, initial_window, consistency)
        consistency = self._consistency(
            source_counter=int(final_info.get("source_counter", 0)),
            dest_counter=int(final_info.get("dest_counter", 0)),
            state_size_bytes=int(final_info.get("state_size_bytes", 0)),
        )
        return (
            MigrationWindow(total_start, total_end),
            MigrationWindow(downtime_start, downtime_end),
            initial_window,
            consistency,
        )

    def _run_postcopy(self, server_a: Container, server_b: Container) -> Tuple[MigrationWindow, MigrationWindow, MigrationWindow, StateConsistency]:
        # Postcopy: disconnect clients first, then transfer, then reconnect
        total_start = now_ts()

        if self.cfg.migration.dest_preboot == False:
            if server_b is None:
                server_b = self.dm.run_server_b() # run server B if not already running
                print("[migration] destination pre-booted before migration")
                self.register_host_ports(server_a, server_b)

        if server_b is None:
            raise ValueError("server_b is not available for postcopy migration")

        source_internal_url = self._internal_url(server_a)


        # Disconnect clients (start downtime)
        downtime_start = now_ts()
        self.dm.drop_alias(server_a)
        print("[postcopy] clients disconnected at", downtime_start - total_start)

        # Transfer full state from A to B while clients disconnected
        pull_info = self._pull_state_remote(server_b, source_internal_url)
        print("[postcopy] state transfer completed at", now_ts() - total_start)

        # Reconnect clients to B
        self.dm.attach_alias(server_b)
        downtime_end = now_ts()
        print("[postcopy] clients reconnected at", downtime_end - total_start)

        total_end = downtime_end
        print("[postcopy] completed post consistency check at", now_ts() - total_start)

        # For postcopy there was no initial background copy; return an empty initial window
        initial_window = MigrationWindow(total_start, total_start)
        consistency = self._consistency(
            source_counter=int(pull_info.get("source_counter", 0)),
            dest_counter=int(pull_info.get("dest_counter", 0)),
            state_size_bytes=int(pull_info.get("state_size_bytes", 0)),
        )
        return (
            MigrationWindow(total_start, total_end),
            MigrationWindow(downtime_start, downtime_end),
            initial_window,
            consistency,
        )

    def _consistency(self, source_counter: int, dest_counter: int, state_size_bytes: int) -> StateConsistency:
        return StateConsistency(
            pre_counter=source_counter,
            post_counter=dest_counter,
            diff=abs(source_counter - dest_counter),
            state_size_bytes=state_size_bytes,
        )
