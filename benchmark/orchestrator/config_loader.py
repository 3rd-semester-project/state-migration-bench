from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Optional

import yaml


Strategy = Literal["precopy", "postcopy", "cold"]


@dataclass(frozen=True)
class ClientsConfig:
    count: int
    rate_hz: float
    payload_bytes: int
    timeout_ms: int = 800


@dataclass(frozen=True)
class NetworkConfig:
    latency_ms: int = 0
    jitter_ms: int = 0
    loss_pct: float = 0.0


@dataclass(frozen=True)
class MigrationConfig:
    strategy: Strategy
    delay_s: float
    postcopy_sync_s: float = 5.0  # used only for postcopy
    dest_preboot: bool = True


@dataclass(frozen=True)
class GeneralConfig:
    run_id: str
    results_dir: str = "results"


@dataclass(frozen=True)
class ServersConfig:
    service_alias: str = "service"
    port: int = 5000
    image_server: str = "migration_test/server:latest"
    image_client: str = "migration_test/client:latest"
    network_name: str = "bench_net"


@dataclass(frozen=True)
class Config:
    general: GeneralConfig
    clients: ClientsConfig
    network: NetworkConfig
    migration: MigrationConfig
    servers: ServersConfig


def _require(cond: bool, msg: str) -> None:
    if not cond:
        raise ValueError(msg)


def load_config(path: str | Path) -> Config:
    p = Path(path)
    data = yaml.safe_load(p.read_text(encoding="utf-8"))

    # Basic schema checks
    _require("general" in data, "Missing 'general'")
    _require("clients" in data, "Missing 'clients'")
    _require("migration" in data, "Missing 'migration'")
    # Optional blocks
    network = data.get("network", {}) or {}
    servers = data.get("servers", {}) or {}

    general = GeneralConfig(
        run_id=str(data["general"]["run_id"]),
        results_dir=str(data["general"].get("results_dir", "results")),
    )

    clients = ClientsConfig(
        count=int(data["clients"]["count"]),
        rate_hz=float(data["clients"]["rate_hz"]),
        payload_bytes=int(data["clients"]["payload_bytes"]),
        timeout_ms=int(data["clients"].get("timeout_ms", 800)),
    )
    _require(clients.count > 0, "clients.count must be > 0")
    _require(clients.rate_hz > 0, "clients.rate_hz must be > 0")
    _require(clients.payload_bytes >= 0, "clients.payload_bytes must be >= 0")

    migration = MigrationConfig(
        strategy=str(data["migration"]["strategy"]).lower(),  # type: ignore
        delay_s=float(data["migration"]["delay_s"]),
        postcopy_sync_s=float(data["migration"].get("postcopy_sync_s", 5.0)),
        dest_preboot=bool(data["migration"].get("dest_preboot", True)),
    )
    _require(migration.strategy in ("precopy", "postcopy", "cold"), "invalid migration.strategy")
    _require(migration.delay_s >= 0, "migration.delay_s must be >= 0")

    network_cfg = NetworkConfig(
        latency_ms=int(network.get("latency_ms", 0)),
        jitter_ms=int(network.get("jitter_ms", 0)),
        loss_pct=float(network.get("loss_pct", 0.0)),
    )

    servers_cfg = ServersConfig(
        service_alias=str(servers.get("service_alias", "service")),
        port=int(servers.get("port", 5000)),
        image_server=str(servers.get("image_server", "migration_test/server:latest")),
        image_client=str(servers.get("image_client", "migration_test/client:latest")),
        network_name=str(servers.get("network_name", "bench_net")),
    )

    return Config(
        general=general,
        clients=clients,
        network=network_cfg,
        migration=migration,
        servers=servers_cfg,
    )


def dump_config_json(cfg: Config) -> str:
    return json.dumps(cfg, default=lambda o: o.__dict__, indent=2, ensure_ascii=False)
