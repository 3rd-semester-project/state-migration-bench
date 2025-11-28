from __future__ import annotations

import os
import threading
import time
from typing import Any, Dict

from flask import Flask, jsonify, request  # type: ignore

app = Flask(__name__)

STATE_LOCK = threading.Lock()
STATE: Dict[str, Any] = {
    "server": os.environ.get("SERVER_NAME", "server"),
    "counter": 0,
    "last_seq": -1,
    "updated_ts": time.time(),
    # track last payload details so state size reflects ingested payloads
    "payload_size": 0,
    "payload_blob": "",
}


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"ok": True, "server": STATE["server"]})


@app.route("/ingest", methods=["POST"])
def ingest():
    payload = request.get_json(force=True, silent=True) or {}
    seq = int(payload.get("seq", -1))
    # Simple idempotency: ignore duplicates
    with STATE_LOCK:
        if seq > STATE.get("last_seq", -1):
            STATE["last_seq"] = seq
            STATE["counter"] = int(STATE.get("counter", 0)) + 1
            # Accumulate payloads: append the client blob and update total size.
            # This makes STATE grow as more client messages are ingested (intended
            # for testing how migration behaves with increasing state sizes).
            try:
                blob = payload.get("blob", "") or ""
            except Exception:
                blob = ""
            try:
                # prefer explicit size if provided, otherwise use actual blob length
                size = int(payload.get("size", len(blob)))
            except Exception:
                size = len(blob)
            # Append blob and update cumulative size
            STATE["payload_blob"] = (STATE.get("payload_blob", "") or "") + blob
            STATE["payload_size"] = int(STATE.get("payload_size", 0)) + len(blob)
            STATE["updated_ts"] = time.time()
    return jsonify({"ack": True, "seq": seq, "server": STATE["server"], "counter": STATE["counter"]})


@app.route("/state", methods=["GET", "POST"])
def state():
    if request.method == "GET":
        with STATE_LOCK:
            s = dict(STATE)
        return jsonify(s)
    data = request.get_json(force=True, silent=True) or {}
    with STATE_LOCK:
        # Merge: keep the max counter and last_seq
        STATE["counter"] = max(int(STATE.get("counter", 0)), int(data.get("counter", 0)))
        STATE["last_seq"] = max(int(STATE.get("last_seq", -1)), int(data.get("last_seq", -1)))
        # If incoming state has a payload_blob/size, prefer the one with larger payload_size
        try:
            incoming_size = int(data.get("payload_size", -1))
        except Exception:
            incoming_size = -1
        if incoming_size >= 0 and incoming_size >= int(STATE.get("payload_size", 0)):
            STATE["payload_size"] = incoming_size
            STATE["payload_blob"] = data.get("payload_blob", "") or ""
        STATE["updated_ts"] = time.time()
    return jsonify({"imported": True})


def main():
    port = int(os.environ.get("PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)


if __name__ == "__main__":
    main()
