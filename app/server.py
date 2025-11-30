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
    # blob now stores a mapping of counter->blob_value so we can track individual ingests
    "blob": {},
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
            STATE["updated_ts"] = time.time()
            # Store the incoming blob under the new counter so each ingest is tracked
            incoming = payload.get("blob", "") or ""
            c = str(STATE["counter"])
            # ensure blob is a dict
            if not isinstance(STATE.get("blob"), dict):
                STATE["blob"] = {}
            STATE["blob"][c] = incoming
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
        STATE["updated_ts"] = time.time()
        # Merge blob dicts: incoming blob should be a mapping of counter->value
        incoming_blob = data.get("blob", {}) or {}
        if not isinstance(incoming_blob, dict):
            # fallback: if remote used old string format, keep existing blob
            incoming_blob = {}
        if not isinstance(STATE.get("blob"), dict):
            STATE["blob"] = {}
        for k, v in incoming_blob.items():
            # prefer to import/overwrite entries from incoming state
            STATE["blob"][k] = v
    return jsonify({"imported": True})


def main():
    port = int(os.environ.get("PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)


if __name__ == "__main__":
    main()
