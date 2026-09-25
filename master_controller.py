"""
Role: acts as the master node. Workers (simulating VM worker nodes)
register with it and send periodic heartbeats. Clients (host PC) call
this node's REST API to create/monitor/delete network slices.

Run:  uvicorn controller:app --port 8000 --reload
"""
import time
import threading
import itertools
from typing import Optional

import requests
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

app = FastAPI(title="Network Slicing Controller")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_methods=["*"], allow_headers=["*"],
)

HEARTBEAT_TIMEOUT = 4.0   # seconds without a heartbeat -> worker considered DOWN
CHECK_INTERVAL = 1.0

lock = threading.Lock()
workers = {}     # worker_id -> {url, last_heartbeat, status}
slices = {}      # slice_id -> {type, bandwidth_mbps, latency_ms, priority, worker_id, status}
_slice_counter = itertools.count(1)
events = []      # simple in-memory event log for the dashboard


def log(msg: str):
    ts = time.strftime("%H:%M:%S")
    events.append(f"[{ts}] {msg}")
    del events[:-50]  # keep last 50


class WorkerRegistration(BaseModel):
    worker_id: str
    url: str


class SliceRequest(BaseModel):
    type: str            # eMBB | URLLC | mMTC
    bandwidth_mbps: float
    latency_ms: float
    priority: int = 1


def pick_worker(exclude: Optional[str] = None):
    """Least-loaded healthy worker (simple load-balancing policy)."""
    candidates = [
        (wid, w) for wid, w in workers.items()
        if w["status"] == "UP" and wid != exclude
    ]
    if not candidates:
        return None
    def load(item):
        wid, _ = item
        return sum(1 for s in slices.values() if s["worker_id"] == wid and s["status"] == "ACTIVE")
    candidates.sort(key=load)
    return candidates[0][0]


def provision_on_worker(worker_id: str, slice_id: str, spec: dict) -> bool:
    w = workers.get(worker_id)
    if not w:
        return False
    try:
        r = requests.post(f"{w['url']}/provision", json={"slice_id": slice_id, **spec}, timeout=2)
        return r.status_code == 200
    except requests.RequestException:
        return False


@app.post("/workers/register")
def register_worker(reg: WorkerRegistration):
    with lock:
        workers[reg.worker_id] = {
            "url": reg.url,
            "last_heartbeat": time.time(),
            "status": "UP",
        }
    log(f"Worker {reg.worker_id} registered at {reg.url}")
    return {"ok": True}


@app.post("/workers/heartbeat/{worker_id}")
def heartbeat(worker_id: str):
    with lock:
        if worker_id not in workers:
            raise HTTPException(404, "unknown worker")
        was_down = workers[worker_id]["status"] == "DOWN"
        workers[worker_id]["last_heartbeat"] = time.time()
        workers[worker_id]["status"] = "UP"
    if was_down:
        log(f"Worker {worker_id} is back UP")
    return {"ok": True}


@app.get("/workers")
def list_workers():
    with lock:
        return {
            wid: {
                "url": w["url"],
                "status": w["status"],
                "seconds_since_heartbeat": round(time.time() - w["last_heartbeat"], 1),
                "slice_count": sum(1 for s in slices.values() if s["worker_id"] == wid and s["status"] == "ACTIVE"),
            }
            for wid, w in workers.items()
        }


@app.post("/slices")
def create_slice(req: SliceRequest):
    worker_id = pick_worker()
    if not worker_id:
        raise HTTPException(503, "no healthy worker nodes available")
    slice_id = f"slice-{next(_slice_counter)}"
    spec = req.dict()
    ok = provision_on_worker(worker_id, slice_id, spec)
    if not ok:
        raise HTTPException(502, "provisioning failed on chosen worker")
    with lock:
        slices[slice_id] = {**spec, "worker_id": worker_id, "status": "ACTIVE"}
    log(f"Created {slice_id} ({req.type}) on {worker_id}")
    return {"slice_id": slice_id, "worker_id": worker_id}


@app.get("/slices")
def list_slices():
    with lock:
        return slices


@app.delete("/slices/{slice_id}")
def delete_slice(slice_id: str):
    with lock:
        s = slices.get(slice_id)
        if not s:
            raise HTTPException(404, "slice not found")
        worker_id = s["worker_id"]
        w = workers.get(worker_id)
    if w:
        try:
            requests.post(f"{w['url']}/deprovision/{slice_id}", timeout=2)
        except requests.RequestException:
            pass
    with lock:
        del slices[slice_id]
    log(f"Deleted {slice_id}")
    return {"ok": True}


@app.get("/events")
def get_events():
    return list(reversed(events))


def monitor_loop():
    while True:
        time.sleep(CHECK_INTERVAL)
        with lock:
            now = time.time()
            down_workers = [
                wid for wid, w in workers.items()
                if w["status"] == "UP" and now - w["last_heartbeat"] > HEARTBEAT_TIMEOUT
            ]
            for wid in down_workers:
                workers[wid]["status"] = "DOWN"
        for wid in down_workers:
            log(f"Worker {wid} missed heartbeats -> marked DOWN, starting recovery")
            # recover its active slices onto other healthy workers
            with lock:
                affected = [sid for sid, s in slices.items()
                            if s["worker_id"] == wid and s["status"] == "ACTIVE"]
            for sid in affected:
                spec = {k: slices[sid][k] for k in ("type", "bandwidth_mbps", "latency_ms", "priority")}
                new_worker = pick_worker(exclude=wid)
                if new_worker and provision_on_worker(new_worker, sid, spec):
                    with lock:
                        slices[sid]["worker_id"] = new_worker
                    log(f"Recovered {sid}: migrated from {wid} -> {new_worker}")
                else:
                    with lock:
                        slices[sid]["status"] = "FAILED"
                    log(f"Could not recover {sid}: no healthy worker available")


threading.Thread(target=monitor_loop, daemon=True).start()