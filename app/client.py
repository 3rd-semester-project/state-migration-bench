from __future__ import annotations

import os
import random
import sys
import time
from typing import Tuple

import requests  # type: ignore

def env_str(name: str, default: str) -> str:
    return os.environ.get(name, default)


def env_int(name: str, default: int) -> int:
    return int(os.environ.get(name, str(default)))


def env_float(name: str, default: float) -> float:
    return float(os.environ.get(name, str(default)))


def main():
    target_host = env_str("TARGET_HOST", "service")
    target_port = env_int("TARGET_PORT", 5000)
    client_id = env_str("CLIENT_ID", "1")
    rate_hz = env_float("RATE_HZ", 5.0)
    payload_bytes = env_int("PAYLOAD_BYTES", 32)
    timeout_ms = env_int("TIMEOUT_MS", 800)
    csv_prefix = env_str("CSV_PREFIX", "run=local")

    base_url = f"http://{target_host}:{target_port}"
    period = 1.0 / max(0.1, rate_hz)

    seq = 0
    rnd = random.Random(42 + int(client_id))
    session = requests.Session()
    next_deadline = time.perf_counter()

    while True:
        seq += 1
        now_wall = time.time()
        payload = {
            "seq": seq,
            "ts": now_wall,
            "size": payload_bytes,
            "blob": "x" * payload_bytes,
        }

        send_ts_perf = time.perf_counter()
        send_ts_wall = time.time()
        try:
            resp = session.post(
                f"{base_url}/ingest",
                json=payload,
                timeout=timeout_ms / 1000.0,
            )
            if resp.ok:
                recv_ts_wall = time.time()
                rtt_ms = (time.perf_counter() - send_ts_perf) * 1000.0
                print(f"CSV:{seq},{send_ts_wall:.6f},{recv_ts_wall:.6f},{rtt_ms:.3f},ok")
            else:
                print(f"CSV:{seq},{send_ts_wall:.6f},,,err_http")
        except Exception:
            print(f"CSV:{seq},{send_ts_wall:.6f},,,err_exc")
        sys.stdout.flush()

        next_deadline += period
        sleep_left = next_deadline - time.perf_counter()
        if sleep_left > 0:
            time.sleep(sleep_left)

if __name__ == "__main__":
    main()
