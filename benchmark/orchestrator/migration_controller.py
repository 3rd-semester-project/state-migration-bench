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
        resp = requests.get(self._url(c, "/state"), timeout=2)
        resp.raise_for_status()
        return resp.json()

    def _import_state(self, c: Container, state: Dict[str, Any]) -> None:
        resp = requests.post(self._url(c, "/state"), json=state, timeout=3)
        resp.raise_for_status()

    # Strategies

    def run(self, server_a: Container, server_b: Container) -> Tuple[MigrationWindow, StateConsistency, int]:
        # Allow initial traffic
        time.sleep(self.cfg.migration.delay_s)

        if self.cfg.migration.strategy == "precopy":
            return self._run_precopy(server_a, server_b)
        if self.cfg.migration.strategy == "postcopy":
            return self._run_postcopy(server_a, server_b)
        return self._run_cold(server_a, server_b)

    def _run_precopy(self, server_a: Container, server_b: Container) -> Tuple[MigrationWindow, StateConsistency, int]:
        pre_state = self._get_state(server_a)
        # Pre-transfer
        self._import_state(server_b, pre_state)

        start = now_ts()
        self.dm.switch_alias_precopy(server_a, server_b)
        end = now_ts()
        # Post consistency check
        post_state = self._get_state(server_b)
        return MigrationWindow(start, end), self._consistency(pre_state, post_state), int(post_state.get("payload_size", 0))

    def _run_postcopy(self, server_a: Container, server_b: Container) -> Tuple[MigrationWindow, StateConsistency, int]:
        start = now_ts()
        self.dm.switch_alias_postcopy(server_a, server_b)
        end = now_ts()

        # Progressive sync for a limited window
        def sync_loop():
            stop_at = time.time() + self.cfg.migration.postcopy_sync_s
            while time.time() < stop_at:
                try:
                    st = self._get_state(server_a)  # A remains connected (no alias)
                    self._import_state(server_b, st)
                except Exception:
                    pass
                time.sleep(0.3)

        t = threading.Thread(target=sync_loop, daemon=True)
        t.start()
        t.join()

        pre_state = self._get_state(server_a)
        post_state = self._get_state(server_b)
        return MigrationWindow(start, end), self._consistency(pre_state, post_state), int(post_state.get("payload_size", 0))

    def _run_cold(self, server_a: Container, server_b: Container) -> Tuple[MigrationWindow, StateConsistency, int]:
        start = now_ts()
        self.dm.switch_alias_cold(server_a, server_b)
        pre_state = self._get_state(server_a)
        # Import into B
        self._import_state(server_b, pre_state)
        # Attach alias to B after import
        self.dm.attach_alias(server_b)
        end = now_ts()
        post_state = self._get_state(server_b)
        return MigrationWindow(start, end), self._consistency(pre_state, post_state), int(post_state.get("payload_size", 0))

    def _consistency(self, pre: Dict[str, Any], post: Dict[str, Any]) -> StateConsistency:
        pre_counter = int(pre.get("counter", 0))
        post_counter = int(post.get("counter", 0))
        return StateConsistency(pre_counter=pre_counter, post_counter=post_counter, diff=abs(pre_counter - post_counter)) 
