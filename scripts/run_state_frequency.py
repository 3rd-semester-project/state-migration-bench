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

CANDIDATE_KEYS = [
    ["clients", "rate_hz"],
]

SIZES_TO_TEST: List[int] = [10, 20, 40, 80, 160]  # example frequencies in Hz

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
        if k in CANDIDATE_KEYS[0]:
            cfg[k] = sizestr
            return True
    return False
def main() -> None:
    if not RESULTS_DIR.exists():
        RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    original_cfg = load_yaml(CONFIG_PATH)
    if not BACKUP_PATH.exists():
        shutil.copy2(CONFIG_PATH, BACKUP_PATH)

    STRATEGIES = ["precopy", "postcopy"]
    for strat in STRATEGIES:
        for size in SIZES_TO_TEST:
            cfg = original_cfg.copy()
            if not find_and_set(cfg, size):
                print(f"Warning: could not find suitable key to set size {size} in config.")
                continue

            # Set run id and strategy for this sweep
            if "general" not in cfg or not isinstance(cfg.get("general"), dict):
                cfg["general"] = {}
            cfg["general"]["run_id"] = "state_frequency"
            if "migration" not in cfg or not isinstance(cfg.get("migration"), dict):
                cfg["migration"] = {}
            cfg["migration"]["strategy"] = strat

            dump_yaml(cfg, CONFIG_PATH)
            print(f"Running benchmark with size {size} strategy {strat}...")

            out_path = LOGS_DIR / f"output_size_{size}_{strat}.log"
            err_path = LOGS_DIR / f"error_size_{size}_{strat}.log"

            # Invoke the benchmark module directly and capture output
            cmd = [PYTHON_EXE, "-m", "benchmark.orchestrator.cli", "-c", str(CONFIG_PATH)]

            try:
                proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            except Exception as e:
                err_text = f"Failed to start command {cmd}: {e}\n"
                err_path.write_text(err_text, encoding="utf-8")
                print(err_text)
                continue

            out, err = proc.communicate()
            out_path.write_text(out or "", encoding="utf-8")
            err_path.write_text(err or "", encoding="utf-8")

            rc = proc.returncode
            if rc is None:
                print(f"Benchmark for size {size} did not complete properly. See {err_path}")
            elif rc != 0:
                print(f"Benchmark for size {size} failed with return code {rc}. See {err_path}")
            else:
                print(f"Benchmark for size {size} completed successfully.")

    # Restore original config
    if BACKUP_PATH.exists():
        shutil.move(str(BACKUP_PATH), str(CONFIG_PATH))
    print("Restored original configuration.")

if __name__ == "__main__":
    main()