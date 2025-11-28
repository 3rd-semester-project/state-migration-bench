from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Literal, Tuple, Dict, Any
import json

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
        self.port = cfg.servers.port

    # HTTP helpers

    def _url(self, ip: str, path: str) -> str:
        return f"http://{ip}:{self.port}{path}"

    def _get_state(self, c: Container) -> Dict[str, Any] | None:
        # Use DockerManager.endpoint_url which prefers published host ports (localhost)
        try:
            url = self.dm.endpoint_url(c, "/state", internal_port=self.port)
        except Exception as e:
            print(f"Could not build endpoint URL for {getattr(c, 'name', '?')}: {e}")
            return None
        print(url)
        try:
            health_url = self.dm.endpoint_url(c, "/health", internal_port=self.port)
            print(f"Checking health of {c.name} at {health_url}")
            health_resp = requests.get(health_url, timeout=2)
            health_resp.raise_for_status()
            print(f"{c.name} is healthy: {health_resp.json()}")
        except requests.exceptions.RequestException as e:
            print(f"Health check failed for {c.name}: {e}")
            return None
        except Exception as e:
            print(f"Could not build health URL for {c.name}: {e}")
            return None
        
        max_retries = 1
        for attempt in range(1, max_retries + 1):
            try:
                resp = requests.get(url, timeout=2)
                resp.raise_for_status()
                return resp.json()
            except requests.exceptions.RequestException as e:
                print(f"Attempt {attempt}/{max_retries}: Error getting state from {c.name}: {e}")
                if attempt < max_retries:
                    time.sleep(0.5 * attempt)
                else:
                    print(f"Failed to get state from {c.name} after {max_retries} attempts")
                    return None

    def _import_state(self, c: Container, state: Dict[str, Any]) -> int:
        if not state:
            print(f"No state provided to import into {getattr(c, 'name', '?')}, skipping import")
            return 0
        try:
            url = self.dm.endpoint_url(c, "/state", internal_port=self.port)
        except Exception as e:
            print(f"Could not build endpoint URL for import target {getattr(c, 'name', '?')}: {e}")
            return 0
        body = json.dumps(state).encode("utf-8")
        size = len(body)
        # Avoid printing the full payload blob to the console; log only size and key metadata
        counter = state.get("counter") if isinstance(state, dict) else None
        last_seq = state.get("last_seq") if isinstance(state, dict) else None
        print(f"Importing state to {c.name} via {url}: size={size} bytes, counter={counter}, last_seq={last_seq}")
        resp = requests.post(url, data=body, headers={"Content-Type": "application/json"}, timeout=3)
        resp.raise_for_status()
        return size

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
        if not pre_state:
            print(f"Precopy aborted: could not fetch state from {server_a.name}")
            return MigrationWindow(0, 0), StateConsistency(pre_counter=0, post_counter=0, diff=0), 0

        # Pre-transfer
        size = self._import_state(server_b, pre_state)

        start = now_ts()
        self.dm.switch_alias_precopy(server_a, server_b)
        end = now_ts()
        # Post consistency check
        post_state = self._get_state(server_b) or {}
        return MigrationWindow(start, end), self._consistency(pre_state, post_state), size

    def _run_postcopy(self, server_a: Container, server_b: Container) -> Tuple[MigrationWindow, StateConsistency, int]:
        start = now_ts()
        self.dm.switch_alias_postcopy(server_a, server_b)
        end = now_ts()

        # Progressive sync for a limited window
        def sync_loop():
            stop_at = time.time() + self.cfg.migration.postcopy_sync_s
            last_size = 0
            while time.time() < stop_at:
                try:
                    st = self._get_state(server_a)  # A remains connected (no alias)
                    if st:
                        last_size_local = self._import_state(server_b, st)
                        if last_size_local:
                            # remember last non-zero import size
                            nonlocal_last_sizes.append(last_size_local)
                except Exception:
                    pass
                time.sleep(0.3)

        # We'll capture last import sizes in a list from the outer scope
        nonlocal_last_sizes: list[int] = []
        t = threading.Thread(target=sync_loop, daemon=True)
        t.start()
        t.join()
        pre_state = self._get_state(server_a) or {}
        post_state = self._get_state(server_b) or {}
        # Prefer last non-zero transferred size recorded during sync
        size = nonlocal_last_sizes[-1] if nonlocal_last_sizes else 0
        return MigrationWindow(start, end), self._consistency(pre_state, post_state), size

    def _run_cold(self, server_a: Container, server_b: Container) -> Tuple[MigrationWindow, StateConsistency, int]:
        start = now_ts()
        self.dm.switch_alias_cold(server_a, server_b)
        pre_state = self._get_state(server_a)
        if not pre_state:
            print(f"Cold migration aborted: could not fetch state from {server_a.name}")
            return MigrationWindow(0, 0), StateConsistency(pre_counter=0, post_counter=0, diff=0), 0

        # Import into B
        size = self._import_state(server_b, pre_state)
        # Attach alias to B after import
        self.dm.attach_alias(server_b)
        end = now_ts()
        post_state = self._get_state(server_b) or {}
        return MigrationWindow(start, end), self._consistency(pre_state, post_state), size

    def _consistency(self, pre: Dict[str, Any], post: Dict[str, Any]) -> StateConsistency:
        pre_counter = int(pre.get("counter", 0))
        post_counter = int(post.get("counter", 0))
        return StateConsistency(pre_counter=pre_counter, post_counter=post_counter, diff=abs(pre_counter - post_counter))
