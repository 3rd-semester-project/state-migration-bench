"""Run benchmark across a set of increasing state sizes by editing configs/base_example.yaml.

This script is intended to run on Windows PowerShell (user's default shell).
It will:
 - load `configs/base_example.yaml`
 - for each configured state size, update the YAML (under the appropriate key) to reflect the desired blob size or number of ingests
 - run the benchmark via: `python3.13.exe -m benchmark.orchestrator.cli -c configs\base_example.yaml`
 - save stdout/stderr and the resulting metrics file (if any) under `results/` with a suffix per size

Notes:
 - Adjust `STATE_SIZE_KEY` if your config uses a different path for initial state size.
 - This script edits the config in-place; it writes a backup named `base_example.yaml.bak` before modifications.
"""
from __future__ import annotations

import subprocess
import sys
import shutil
from pathlib import Path
import yaml
from typing import List, Any

REPO_ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = REPO_ROOT / "configs" / "base_example.yaml"
BACKUP_PATH = CONFIG_PATH.with_suffix(".yaml.bak")
RESULTS_DIR = REPO_ROOT / "results"
LOGS_DIR = REPO_ROOT / "logs"
# Resolve Python executable: prefer explicit `python3.13.exe`, fall back to system python
PYTHON_EXE = shutil.which("python3.13.exe") or shutil.which("python3.13") or shutil.which("python") or sys.executable

# Edit this to target the right keys in your YAML config. Example assumes there's a key for
# server initial blob size or number of initial ingests under 'servers.initial_state_size'
# or similar.
# We'll search for common candidate keys and set the value when found.

CANDIDATE_KEYS = [
    ["migration", "delay_s"],
]

# Migration strategies to test
STRATEGIES = ["precopy", "postcopy"]

# Sizes to test (bytes or count depending on config semantics)
TIMES_TO_TEST: List[int] = [5, 10, 20, 30, 50, 100, 200]


def load_yaml(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def dump_yaml(obj: Any, path: Path) -> None:
    with path.open("w", encoding="utf-8") as f:
        yaml.safe_dump(obj, f, sort_keys=False)


def set_nested_key(cfg: dict, key_path: List[str], value: Any) -> bool:
    """Set value at nested key path if exists. Return True if set."""
    cur = cfg
    for k in key_path[:-1]:
        if not isinstance(cur, dict) or k not in cur:
            return False
        cur = cur[k]
    last = key_path[-1]
    if isinstance(cur, dict) and last in cur:
        cur[last] = value
        return True
    return False


def find_and_set(cfg: dict, sizestr: int) -> bool:
    # Try candidate keys first
    for path in CANDIDATE_KEYS:
        if set_nested_key(cfg, path, sizestr):
            return True
    # Fallback: try top-level keys
    for k in cfg:
        if isinstance(cfg[k], (int, float)):
            cfg[k] = sizestr
            return True
    return False


def run_one(size: int, strategy: str) -> int:
    print(f"Running benchmark for size={size} strategy={strategy}")
    # Load config
    cfg = load_yaml(CONFIG_PATH)

    # Update config
    ok = find_and_set(cfg, size)
    if not ok:
        print("Warning: couldn't find a suitable key to set state size in config. File unchanged.")
    # Set run id and migration strategy for this sweep
    if "general" not in cfg or not isinstance(cfg.get("general"), dict):
        cfg["general"] = {}
    cfg["general"]["run_id"] = "state_size2"
    if "migration" not in cfg or not isinstance(cfg.get("migration"), dict):
        cfg["migration"] = {}
    cfg["migration"]["strategy"] = strategy
    # Backup original on first run
    if not BACKUP_PATH.exists():
        shutil.copy2(CONFIG_PATH, BACKUP_PATH)

    dump_yaml(cfg, CONFIG_PATH)

    # Run the benchmark command
    cmd = [PYTHON_EXE, "-m", "benchmark.orchestrator.cli", "-c", str(CONFIG_PATH)]

    # Prepare output paths
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    out_path = LOGS_DIR / f"run_output_size_{size}_{strategy}.log"
    err_path = LOGS_DIR / f"run_error_size_{size}_{strategy}.log"

    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    except Exception as e:
        err_text = f"Failed to start command {cmd}: {e}\n"
        err_path.write_text(err_text, encoding="utf-8")
        print(err_text)
        return 127

    out, err = proc.communicate()

    out_path.write_text(out or "", encoding="utf-8")
    err_path.write_text(err or "", encoding="utf-8")

    print(f"Saved stdout -> {out_path}")
    print(f"Saved stderr -> {err_path}")

    rc = proc.returncode
    if rc is None:
        rc = -1
    return rc


def main() -> None:
    # Ensure config exists
    if not CONFIG_PATH.exists():
        print(f"Config not found at {CONFIG_PATH}")
        sys.exit(2)

    failures = []
    for strat in STRATEGIES:
        for s in TIMES_TO_TEST:
            rc = run_one(s, strat)
            if rc != 0:
                failures.append(((strat, s), rc))

    # Restore backup
    if BACKUP_PATH.exists():
        shutil.move(str(BACKUP_PATH), str(CONFIG_PATH))

    if failures:
        print("Some runs failed:")
        for (strat, s), rc in failures:
            print(f" - strategy={strat} size={s}, rc={rc}")
        sys.exit(1)
    print("All runs finished successfully.")


if __name__ == "__main__":
    main()
