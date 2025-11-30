from __future__ import annotations

import argparse
from pathlib import Path

from .config_loader import load_config
from .docker_manager import DockerManager, RunningSet
from .migration_controller import MigrationController
from .metrics_collector import MetricsCollector
from .reporter import Reporter
from .utils import ensure_dir


def run_benchmark(config_path: str) -> None:
    cfg = load_config(config_path)
    ensure_dir(Path(cfg.general.results_dir))

    dm = DockerManager(cfg)
    dm.build_images()
    dm.ensure_network()
    server_a, server_b = dm.run_servers()
    clients = dm.run_clients()
    run = RunningSet(server_a=server_a, server_b=server_b, clients=clients)

    try:
        mc = MigrationController(cfg, dm)
        mc.register_host_ports(server_a, server_b)

        win, consistency, state_size = mc.run(server_a, server_b)

        metrics = MetricsCollector(cfg).collect(
            containers=clients, win=win, state_diff=state_size, strategy=cfg.migration.strategy
        )
        reporter = Reporter(cfg)
        csv_path = reporter.save_metrics_csv(metrics)
        print(f"Saved: {csv_path}")
    finally:
        dm.stop_and_cleanup(run)

def main():
    parser = argparse.ArgumentParser(description="Run migration benchmark")
    parser.add_argument("-c", "--config", required=True, help="Path to YAML config")
    args = parser.parse_args()
    run_benchmark(args.config)

if __name__ == "__main__":
    main()
