from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Optional, List, Dict

import docker
from docker.models.containers import Container  # type: ignore

from .config_loader import Config
from .utils import now_ts


@dataclass
class RunningSet:
    server_a: Container
    server_b: Container
    clients: list[Container]


class DockerManager:
    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg
        self.client = docker.from_env()
        self.network = None

    # Images

    def build_images(self) -> None:
        # Server image
        self.client.images.build(
            path=str(self._app_dir()),
            dockerfile="Dockerfile.server",
            tag=self.cfg.servers.image_server,
            rm=True,
        )
        # Client image
        self.client.images.build(
            path=str(self._app_dir()),
            dockerfile="Dockerfile.client",
            tag=self.cfg.servers.image_client,
            rm=True,
        )

    # Network

    def ensure_network(self) -> None:
        name = self.cfg.servers.network_name
        try:
            self.network = self.client.networks.get(name)
        except docker.errors.NotFound:
            self.network = self.client.networks.create(name, driver="bridge")

    def _app_dir(self):
        # Repo root: /home/thomas/Work/lab/migration_test
        # App dir: /home/thomas/Work/lab/migration_test/app
        import pathlib
        return pathlib.Path(__file__).resolve().parents[2] / "app"

    # Helpers for safe (re)attachment

    def _remove_if_exists(self, name: str) -> None:
        try:
            c = self.client.containers.get(name)
            self._safe_stop(c)
        except docker.errors.NotFound:
            pass

    def _is_in_network(self, container: Container) -> bool:
        container.reload()
        net_name = self.cfg.servers.network_name
        return net_name in (container.attrs.get("NetworkSettings", {}).get("Networks") or {})

    def log_container_info(self, container: Container) -> None:
        try:
            container.reload()
            nets = container.attrs.get("NetworkSettings", {}).get("Networks") or {}
            print(f"Container '{container.name}': status={container.status}")
            print(f"  Attached networks: {list(nets.keys())}")
            for net_name, info in nets.items():
                ip = info.get("IPAddress")
                aliases = info.get("Aliases")
                print(f"    - {net_name}: IPAddress={ip}, Aliases={aliases}")
        except Exception as e:
            print(f"Error reading container info for {getattr(container, 'name', '?')}: {e}")

    def wait_for_container_attached(self, container: Container, timeout_s: float = 10.0, interval_s: float = 0.5) -> bool:
        """Wait until the container is attached to the configured network. Returns True if attached."""
        start = time.time()
        while time.time() - start < timeout_s:
            try:
                if self._is_in_network(container):
                    self.log_container_info(container)
                    return True
            except Exception:
                pass
            time.sleep(interval_s)
        # Final info dump
        self.log_container_info(container)
        return False

    def _disconnect_if_connected(self, container: Container) -> None:
        try:
            if self._is_in_network(container):
                self.network.disconnect(container)
        except Exception:
            pass

    def attach_alias(self, container: Container) -> None:
        # Disconnect then connect with alias
        self._disconnect_if_connected(container)
        self.network.connect(container, aliases=[self.cfg.servers.service_alias])

    def drop_alias(self, container: Container) -> None:
        # Disconnect then connect without alias (still reachable for orchestrator)
        self._disconnect_if_connected(container)
        self.network.connect(container)

    # Containers

    def run_servers(self) -> tuple[Container, Container]:
        port = self.cfg.servers.port
        net = self.cfg.servers.network_name

        # Ensure previous conflicting containers are gone
        self._remove_if_exists("server_a")
        self._remove_if_exists("server_b")

        # Publish server ports to host so the orchestrator (running on the host)
        # can reach container endpoints via localhost. Map server_a -> host:port,
        # server_b -> host:(port+1).
        host_port_a = port
        host_port_b = port + 1
        server_a = self.client.containers.run(
            self.cfg.servers.image_server,
            name="server_a",
            detach=True,
            environment={
                "SERVER_NAME": "server_a",
                "PORT": str(port),
            },
            network=net,
            ports={f"{port}/tcp": host_port_a},
        )
        server_b = self.client.containers.run(
            self.cfg.servers.image_server,
            name="server_b",
            detach=True,
            environment={
                "SERVER_NAME": "server_b",
                "PORT": str(port),
            },
            network=net,
            ports={f"{port}/tcp": host_port_b},
        )
        # Give both time to start
        time.sleep(1.0)
        # Ensure alias points to A (reconnect with alias)
        self.attach_alias(server_a)
        try:
            print(f"Published server_a -> localhost:{host_port_a}")
            print(f"Published server_b -> localhost:{host_port_b}")
        except Exception:
            pass
        return server_a, server_b

    def run_clients(self) -> list[Container]:
        env = {
            "TARGET_HOST": self.cfg.servers.service_alias,
            "TARGET_PORT": str(self.cfg.servers.port),
            "RATE_HZ": str(self.cfg.clients.rate_hz),
            "PAYLOAD_BYTES": str(self.cfg.clients.payload_bytes),
            "TIMEOUT_MS": str(self.cfg.clients.timeout_ms),
            "CSV_PREFIX": f"run={self.cfg.general.run_id}",
        }
        clients: list[Container] = []
        for i in range(self.cfg.clients.count):
            name = f"client_{i+1}"
            self._remove_if_exists(name)
            c = self.client.containers.run(
                self.cfg.servers.image_client,
                name=name,
                detach=True,
                environment=env | {"CLIENT_ID": str(i + 1)},
                network=self.cfg.servers.network_name,
            )
            clients.append(c)
        return clients

    def switch_alias_precopy(self, server_a: Container, server_b: Container) -> None:
        # Drop alias from A (keep it connected), then attach alias to B
        self.drop_alias(server_a)
        self.attach_alias(server_b)

    def switch_alias_postcopy(self, server_a: Container, server_b: Container) -> None:
        # Same sequence to avoid "endpoint exists" and keep A reachable for sync
        self.drop_alias(server_a)
        self.attach_alias(server_b)

    def switch_alias_cold(self, server_a: Container, server_b: Container) -> None:
        # Cold cut: remove alias from A (keeps A connected for state snapshot)
        self.drop_alias(server_a)

    def stop_and_cleanup(self, run: RunningSet) -> None:
        # Stop clients first
        for c in run.clients:
            self._safe_stop(c)
        # Stop servers
        self._safe_stop(run.server_a)
        self._safe_stop(run.server_b)
        # Remove network alias if any lingering
        # Network cleanup left to user to preserve logs; can be pruned manually

    def _safe_stop(self, container: Container) -> None:
        try:
            container.reload()
            container.stop(timeout=2)
        except Exception:
            pass
        try:
            container.remove(force=True)
        except Exception:
            pass

    # Helper

    def get_container_ip(self, container: Container) -> str:
        try:
            container.reload()
            net_name = self.cfg.servers.network_name
            nets = container.attrs.get("NetworkSettings", {}).get("Networks") or {}
            if net_name not in nets:
                raise RuntimeError(f"Container '{container.name}' not attached to network '{net_name}'; attachments={list(nets.keys())}")
            ip = nets[net_name].get("IPAddress")
            print(f"Resolved IP for {container.name} on network '{net_name}': {ip}")
            return ip
        except Exception as e:
            print(f"Error obtaining IP for {getattr(container, 'name', '?')}: {e}")
            raise

    def get_published_port(self, container: Container, internal_port: int) -> Optional[int]:
        """Return the host port that the container's internal port is published to, or None."""
        try:
            container.reload()
            ports = container.attrs.get("NetworkSettings", {}).get("Ports") or {}
            key = f"{internal_port}/tcp"
            mapping = ports.get(key)
            if not mapping:
                return None
            # mapping is a list of dicts like [{'HostIp': '0.0.0.0', 'HostPort': '5000'}]
            host_port = int(mapping[0]["HostPort"])
            print(f"Container '{container.name}' internal port {internal_port} published on host port {host_port}")
            return host_port
        except Exception as e:
            print(f"Error reading published ports for {getattr(container, 'name', '?')}: {e}")
            return None

    def endpoint_url(self, container: Container, path: str, internal_port: int | None = None) -> str:
        """Return a URL usable from the orchestrator process to reach the container's HTTP endpoint.
        If the container has the internal_port published to the host, return localhost:hostport, otherwise use internal IP.
        """
        port = internal_port or self.cfg.servers.port
        host_port = self.get_published_port(container, port)
        if host_port:
            return f"http://localhost:{host_port}{path}"
        # fallback to container IP
        ip = self.get_container_ip(container)
        return f"http://{ip}:{port}{path}"
