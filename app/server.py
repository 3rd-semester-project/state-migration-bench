from __future__ import annotations

import os
import threading
import time
from typing import Any, Dict, Optional

import requests  # type: ignore
from flask import Flask, jsonify, request  # type: ignore

app = Flask(__name__)

STATE_LOCK = threading.Lock()
STATE: Dict[str, Any] = {
    "server": os.environ.get("SERVER_NAME", "server"),
    "counter": 0,
    "last_seq": -1,
    "updated_ts": time.time(),
    # blob now stores a mapping of counter->blob_value so we can track individual ingests
    "blob": {},
}


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"ok": True, "server": STATE["server"]})


def _blob_size_bytes(blob: Any) -> int:
    # blob is expected to be a mapping of counter->value
    if not isinstance(blob, dict):
        return 0
    total = 0
    for v in blob.values():
        try:
            total += len(v or "")
        except Exception:
            continue
    return total


def _merge_state(data: Dict[str, Any]) -> None:
    """Merge incoming state payload into local STATE under the lock."""
    incoming_blob = data.get("blob", {}) or {}
    if not isinstance(incoming_blob, dict):
        incoming_blob = {}
    with STATE_LOCK:
        STATE["counter"] = max(int(STATE.get("counter", 0)), int(data.get("counter", 0)))
        STATE["last_seq"] = max(int(STATE.get("last_seq", -1)), int(data.get("last_seq", -1)))
        STATE["updated_ts"] = time.time()
        if not isinstance(STATE.get("blob"), dict):
            STATE["blob"] = {}
        for k, v in incoming_blob.items():
            STATE["blob"][k] = v


@app.route("/ingest", methods=["POST"])
def ingest():
    payload = request.get_json(force=True, silent=True) or {}
    seq = int(payload.get("seq", -1))
    # Simple idempotency: ignore duplicates
    with STATE_LOCK:
        if seq > STATE.get("last_seq", -1):
            STATE["last_seq"] = seq
            STATE["counter"] = int(STATE.get("counter", 0)) + 1
            STATE["updated_ts"] = time.time()
            # Store the incoming blob under the new counter so each ingest is tracked
            incoming = payload.get("blob", "") or ""
            c = str(STATE["counter"])
            # ensure blob is a dict
            if not isinstance(STATE.get("blob"), dict):
                STATE["blob"] = {}
            STATE["blob"][c] = incoming
    return jsonify({"ack": True, "seq": seq, "server": STATE["server"], "counter": STATE["counter"]})


@app.route("/state_meta", methods=["GET"])
def state_meta():
    # Lightweight state view without blobs to avoid large payloads
    with STATE_LOCK:
        return jsonify({
            "server": STATE.get("server"),
            "counter": int(STATE.get("counter", 0)),
            "last_seq": int(STATE.get("last_seq", -1)),
            "updated_ts": float(STATE.get("updated_ts", 0.0)),
        })


@app.route("/state", methods=["GET", "POST"])
def state():
    if request.method == "GET":
        with STATE_LOCK:
            s = dict(STATE)
        return jsonify(s)
    data = request.get_json(force=True, silent=True) or {}
    _merge_state(data)
    return jsonify({"imported": True})


@app.route("/pull_state", methods=["POST", "GET"])
def pull_state():
    """
    Triggered on the destination server (e.g., server_b) to pull state directly
    from another server (e.g., server_a) using its URL. This keeps the full
    state transfer inside the Docker network instead of routing through the host.
    Optional filtering allows partial copies by counter.
    """
    if request.method == "GET":
        payload = {
            "source_url": request.args.get("source_url"),
            "min_counter_exclusive": request.args.get("min_counter_exclusive"),
            "max_counter_inclusive": request.args.get("max_counter_inclusive"),
        }
    else:
        payload = request.get_json(force=True, silent=True) or {}

    source_url = (payload.get("source_url") or "").rstrip("/")
    if not source_url:
        return jsonify({"imported": False, "error": "missing source_url"}), 400

    def _as_int(val: Optional[Any]) -> Optional[int]:
        try:
            return int(val) if val is not None else None
        except Exception:
            return None

    min_counter_exclusive = _as_int(payload.get("min_counter_exclusive"))
    max_counter_inclusive = _as_int(payload.get("max_counter_inclusive") or payload.get("max_counter"))

    try:
        remote = requests.get(f"{source_url}/state", timeout=30)
        remote.raise_for_status()
        remote_state: Dict[str, Any] = remote.json()
    except Exception as exc:
        return jsonify({"imported": False, "error": f"failed to fetch source state: {exc}"}), 502

    remote_counter = int(remote_state.get("counter", 0))
    remote_last_seq = int(remote_state.get("last_seq", -1))
    remote_blob = remote_state.get("blob", {}) or {}
    filtered_blob: Dict[str, Any] = {}
    if isinstance(remote_blob, dict):
        for k, v in remote_blob.items():
            try:
                key_int = int(k)
            except Exception:
                continue
            if min_counter_exclusive is not None and key_int <= min_counter_exclusive:
                continue
            if max_counter_inclusive is not None and key_int > max_counter_inclusive:
                continue
            filtered_blob[k] = v

    import_payload = {
        "counter": remote_counter,
        "last_seq": remote_last_seq,
        "blob": filtered_blob,
    }
    _merge_state(import_payload)

    state_size_bytes = _blob_size_bytes(remote_blob)
    with STATE_LOCK:
        dest_counter = int(STATE.get("counter", 0))
    return jsonify(
        {
            "imported": True,
            "state_size_bytes": state_size_bytes,
            "source_counter": remote_counter,
            "dest_counter": dest_counter,
            "server": STATE.get("server"),
        }
    )


def main():
    port = int(os.environ.get("PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)


if __name__ == "__main__":
    main()
