
import json
import os
import time

_LOG_FP = None

def init_persistent_log(node_id: int, base_dir: str = "logs"):
    global _LOG_FP
    os.makedirs(base_dir, exist_ok=True)
    path = os.path.join(base_dir, f"node_{node_id}.jsonl")
    _LOG_FP = open(path, "a", encoding="utf-8")
    _LOG_FP.write(json.dumps({"ts": time.time(), "type": "LOG_OPEN", "node": node_id}) + "\n")
    _LOG_FP.flush()

def log_event(event: dict):
    if _LOG_FP is None:
        return
    _LOG_FP.write(json.dumps(event) + "\n")
    _LOG_FP.flush()
