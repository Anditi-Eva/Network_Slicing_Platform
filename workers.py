import os
import time
import threading

import requests
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

WORKER_ID = os.environ.get("WORKER_ID", "worker1")
WORKER_PORT = os.environ.get("WORKER_PORT", "8001")
WORKER_URL = f"http://127.0.0.1:{WORKER_PORT}"
CONTROLLER_URL = os.environ.get("CONTROLLER_URL", "http://127.0.0.1:8000")

app = FastAPI(title=f"Worker Node {WORKER_ID}")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_methods=["*"], allow_headers=["*"],
)

local_slices = {}
alive = threading.Event()
alive.set()  # heartbeat is running by default


class SliceSpec(BaseModel):
    slice_id: str
    type: str
    bandwidth_mbps: float
    latency_ms: float
    priority: int = 1


@app.post("/provision")
def provision(spec: SliceSpec):
    local_slices[spec.slice_id] = spec.dict()
    return {"ok": True}


@app.post("/deprovision/{slice_id}")
def deprovision(slice_id: str):
    local_slices.pop(slice_id, None)
    return {"ok": True}


@app.get("/status")
def status():
    return {"worker_id": WORKER_ID, "alive": alive.is_set(), "slices": local_slices}


@app.post("/crash")
def crash():
    """Simulate a VM/node failure: stop sending heartbeats."""
    alive.clear()
    return {"ok": True, "message": f"{WORKER_ID} heartbeat stopped (simulated crash)"}


@app.post("/recover")
def recover():
    """Simulate the node coming back online."""
    alive.set()
    return {"ok": True, "message": f"{WORKER_ID} heartbeat resumed"}


def heartbeat_loop():
    # initial registration, retry until controller is reachable
    while True:
        try:
            requests.post(f"{CONTROLLER_URL}/workers/register",
                          json={"worker_id": WORKER_ID, "url": WORKER_URL}, timeout=2)
            break
        except requests.RequestException:
            time.sleep(1)

    while True:
        if alive.is_set():
            try:
                requests.post(f"{CONTROLLER_URL}/workers/heartbeat/{WORKER_ID}", timeout=2)
            except requests.RequestException:
                pass
        time.sleep(1)


threading.Thread(target=heartbeat_loop, daemon=True).start()