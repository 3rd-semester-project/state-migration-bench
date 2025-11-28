"""
Run the benchmark multiple times with different client payload sizes.

Behavior:
- Edits `configs/base_example.yaml`'s `clients.payload_bytes` and `general.run_id` for each run.
- Runs the orchestrator via the same Python interpreter: `python -m benchmark.orchestrator.cli -c configs/base_example.yaml`.
- Waits for the run to complete, then checks Docker to ensure `server_a` and `server_b` are not running.
- Restores the original config file on exit (even on error).

Usage:
    python scripts/run_sweep.py

Requires the Docker SDK for Python and PyYAML (the repo already uses these).
"""
from __future__ import annotations

import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import docker
import yaml

CONFIG_PATH = Path(__file__).resolve().parents[1] / "configs" / "base_example.yaml"
SIZES = [1024, 16384, 65536, 262144, 1048576, 4194304, 16777216]  # in bytes
SERVER_NAMES = ["server_a", "server_b"]

# How long to wait for containers to stop (seconds)
CONTAINER_STOP_TIMEOUT = 90


def load_config(path: Path) -> Dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def write_config(path: Path, data: Dict[str, Any]) -> None:
    path.write_text(yaml.safe_dump(data, sort_keys=False), encoding="utf-8")


def ensure_containers_stopped(client: docker.DockerClient, timeout: float = CONTAINER_STOP_TIMEOUT) -> None:
    """Wait until server containers are not running. If still present after timeout, attempt to remove them."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        all_stopped = True
        for name in SERVER_NAMES:
            try:
                c = client.containers.get(name)
                c.reload()
                # statuses: created, restarting, running, removing, paused, exited, dead
                if c.status == "running" or c.status == "restarting":
                    all_stopped = False
                else:
                    # it's present but not running (exited) - remove to be clean
                    try:
                        c.remove(force=True)
                    except Exception:
                        pass
            except docker.errors.NotFound:
                # container not present
                continue
            except Exception:
                # keep waiting; possibly transient
                all_stopped = False
        if all_stopped:
            return
        time.sleep(1.0)
    # deadline passed: try to force remove any remaining
    for name in SERVER_NAMES:
        try:
            c = client.containers.get(name)
            try:
                c.remove(force=True)
                print(f"Force-removed container {name}")
            except Exception as e:
                print(f"Failed to remove {name}: {e}")
        except docker.errors.NotFound:
            pass


def run_one(size: int, original_cfg: Dict[str, Any]) -> int:
    cfg = load_config(CONFIG_PATH)
    # Update payload size
    cfg.setdefault("clients", {})
    cfg["clients"]["payload_bytes"] = int(size)
    # Unique run_id
    run_id = f"run_{size}_{datetime.now().strftime('%Y%m%dT%H%M%S')}"
    cfg.setdefault("general", {})
    cfg["general"]["run_id"] = run_id

    print(f"\n=== Running payload size={size} (run_id={run_id}) ===")
    write_config(CONFIG_PATH, cfg)

    # Run the benchmark using the same Python interpreter
    cmd = [sys.executable, "-m", "benchmark.orchestrator.cli", "-c", str(CONFIG_PATH)]
    print("Running:", " ".join(cmd))

    start = time.time()
    proc = subprocess.run(cmd)
    dur = time.time() - start
    print(f"Run finished with returncode={proc.returncode} in {dur:.1f}s")
    return proc.returncode


def main():
    orig = load_config(CONFIG_PATH)
    client = docker.from_env()

    try:
        for sz in SIZES:
            rc = run_one(sz, orig)

            # Wait for containers to stop/cleanup before next run
            print("Waiting for containers to stop/cleanup...")
            ensure_containers_stopped(client)
            print("Containers are stopped (or removed).")

            if rc != 0:
                print(f"Run for size={sz} failed (rc={rc}). Stopping sweep.")
                break

            # small pause between runs
            time.sleep(1.0)
    finally:
        # restore original config
        try:
            write_config(CONFIG_PATH, orig)
            print(f"Restored original config at {CONFIG_PATH}")
        except Exception as e:
            print(f"Failed to restore original config: {e}")
        # Attempt to generate analysis plot after the sweep
        try:
            print("Running analysis plot script to update precopy plots...")
            plot_script = Path(__file__).resolve().parents[1] / "scripts" / "plot_precopy.py"
            proc = subprocess.run([sys.executable, str(plot_script)], check=False)
            print(f"Plot script exited with code {getattr(proc, 'returncode', None)}")
        except Exception as e:
            print(f"Failed to run plot script: {e}")


if __name__ == "__main__":
    main()
