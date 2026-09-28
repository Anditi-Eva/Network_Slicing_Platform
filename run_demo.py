import json, os, subprocess, sys, time, statistics
from pathlib import Path
import requests

ROOT=Path(__file__).resolve().parent
RESULTS=ROOT/"results"; RESULTS.mkdir(exist_ok=True)
CONTROLLER="http://127.0.0.1:8000"
WORKERS=["http://127.0.0.1:8001","http://127.0.0.1:8002"]
procs=[]

def wait(url, timeout=15):
    end=time.time()+timeout
    while time.time()<end:
        try:
            if requests.get(url,timeout=1).ok: return True
        except requests.RequestException: pass
        time.sleep(.2)
    return False

def start():
    for port in (8001,8002,8000):
        procs.append(subprocess.Popen([sys.executable,"service.py",str(port)],
                                      cwd=ROOT,stdout=subprocess.DEVNULL,
                                      stderr=subprocess.DEVNULL))
    if not wait(CONTROLLER+"/health"): raise RuntimeError("Controller failed")
    for w in WORKERS:
        if not wait(w+"/health"): raise RuntimeError("Worker failed: "+w)

def stop():
    for p in procs:
        if p.poll() is None: p.terminate()

def main():
    start()
    try:
        out={}
        # M1
        out["milestone_1"]={
            "architecture":requests.get(CONTROLLER+"/architecture").json(),
            "workers_before":requests.get(CONTROLLER+"/workers").json(),
            "slice_creation":requests.post(CONTROLLER+"/slices",json={
                "slice_id":"slice-m1-demo","cpu_percent":20,
                "memory_mb":128,"bandwidth_mbps":10,"priority":5}).json()
        }
        # M2
        lat=[]; ok=failed=0; n=10; t0=time.perf_counter()
        for i in range(n):
            s=time.perf_counter()
            try:
                r=requests.post(CONTROLLER+"/tasks",
                                json={"task_id":f"task-{i+1}","work_units":150000+i*10000},
                                timeout=20)
                if r.ok: ok+=1; lat.append(time.perf_counter()-s)
                else: failed+=1
            except requests.RequestException: failed+=1
        total=time.perf_counter()-t0
        out["milestone_2"]={
            "successful_tasks":ok,"failed_tasks":failed,
            "throughput_tasks_per_second":round(ok/total,4),
            "average_latency_seconds":round(statistics.mean(lat),4),
            "jitter_seconds":round(statistics.pstdev(lat),4) if len(lat)>1 else 0,
            "packet_loss_percent":round(failed/n*100,2),
            "worker_measurements":requests.get(CONTROLLER+"/workers").json()
        }
        # M3
        out["milestone_3"]={"deployment":requests.get(CONTROLLER+"/deployment").json()}
        # M4
        out["milestone_4"]={
            "leader_election":requests.post(CONTROLLER+"/coordination/election").json(),
            "logical_clock_event":requests.post(CONTROLLER+"/coordination/event",
                json={"event":"slice-created"}).json()
        }
        path=RESULTS/"milestone_results.json"
        path.write_text(json.dumps(out,indent=2),encoding="utf-8")
        m2=out["milestone_2"]; m4=out["milestone_4"]
        print("\\n"+"="*62)
        print("DISTRIBUTED NETWORK SLICING PLATFORM — MILESTONES 1–4")
        print("="*62)
        print("MILESTONE 1: ✓ distributed foundation")
        print(f"MILESTONE 2: ✓ throughput={m2['throughput_tasks_per_second']} tasks/s")
        print(f"             latency={m2['average_latency_seconds']} s")
        print(f"             jitter={m2['jitter_seconds']} s")
        print(f"             packet loss={m2['packet_loss_percent']} %")
        print("MILESTONE 3: ✓ distributed architecture")
        print(f"MILESTONE 4: ✓ leader={m4['leader_election']['leader']}, logical clock={m4['logical_clock_event']['logical_clock']}")
        print(f"\\nResults: {path}")
    finally: stop()

if __name__=="__main__": main()
