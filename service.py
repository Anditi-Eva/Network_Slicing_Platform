import sys,time,socket,threading
from multiprocessing import Process,Queue
import psutil,requests,uvicorn
from fastapi import FastAPI
from pydantic import BaseModel

PORT=int(sys.argv[1]) if len(sys.argv)>1 else 8001
NODE="controller" if PORT==8000 else ("node1" if PORT==8001 else "node2")
app=FastAPI(title=f"Network Slice {NODE}")
logical_clock=0; lock=threading.Lock()
allocated={}
workers=["http://127.0.0.1:8001","http://127.0.0.1:8002"]
rr=0; rrlock=threading.Lock()

class SliceRequest(BaseModel):
    slice_id:str; cpu_percent:int; memory_mb:int; bandwidth_mbps:int; priority:int=1
class TaskRequest(BaseModel):
    task_id:str; work_units:int=100000

def tick():
    global logical_clock
    with lock: logical_clock+=1; return logical_clock

def cpu_work(n):
    total=0
    for i in range(n): total+=(i*i)%9973
    return total

def child(n,q): q.put(cpu_work(n))

@app.get("/health")
def health(): return {"node":NODE,"status":"online"}

@app.get("/status")
def status():
    vm=psutil.virtual_memory()
    return {"node":NODE,"host":socket.gethostname(),
            "cpu_percent":psutil.cpu_percent(.05),"memory_percent":vm.percent,
            "available_memory_mb":round(vm.available/1024/1024,2),
            "allocated_slices":len(allocated),"logical_clock":logical_clock}

@app.post("/allocate")
def allocate(req:SliceRequest):
    tick(); allocated[req.slice_id]=req.model_dump()
    return {"status":"allocated","node":NODE,"slice":req.model_dump(),"logical_clock":logical_clock}

@app.post("/task")
def task(req:TaskRequest):
    tick(); q=Queue(); t=time.perf_counter()
    p=Process(target=child,args=(req.work_units,q)); p.start()
    result=q.get(timeout=20); p.join(timeout=2)
    return {"task_id":req.task_id,"node":NODE,"result":result,
            "execution_time_seconds":time.perf_counter()-t,"logical_clock":logical_clock}

@app.post("/clock/event")
def clock_event(payload:dict):
    global logical_clock
    with lock: logical_clock=max(logical_clock,int(payload.get("clock",0)))+1
    return {"node":NODE,"logical_clock":logical_clock,"event":payload.get("event")}

@app.get("/architecture")
def architecture():
    return {"client":"VS Code/API client","controller":{"port":8000,"role":"Core / Controller"},
            "workers":[{"name":"node1","url":workers[0],"role":"Edge / Worker"},
                       {"name":"node2","url":workers[1],"role":"Edge / Worker"}],
            "communication":"REST/HTTP"}

@app.get("/deployment")
def deployment():
    return {"layers":[{"layer":"Client","component":"API client"},
                      {"layer":"Core","component":"Controller / Scheduler / Resource Manager"},
                      {"layer":"Edge","component":"Worker node1"},
                      {"layer":"Edge","component":"Worker node2"}]}

@app.get("/workers")
def workers_status():
    out=[]
    for w in workers:
        try: out.append(requests.get(w+"/status",timeout=2).json())
        except requests.RequestException: out.append({"worker":w,"status":"offline"})
    return out

@app.post("/slices")
def create_slice(req:SliceRequest):
    global rr
    candidates=[]
    for w in workers:
        try:
            s=requests.get(w+"/status",timeout=2).json()
            if s["cpu_percent"]<90 and s["memory_percent"]<90: candidates.append(w)
        except requests.RequestException: pass
    if not candidates: return {"status":"rejected","reason":"No available worker"}
    with rrlock: target=candidates[rr%len(candidates)]; rr+=1
    r=requests.post(target+"/allocate",json=req.model_dump(),timeout=3)
    return {"status":"created","selected_worker":target,"allocation":r.json(),
            "scheduler":"resource-aware round-robin"}

@app.post("/tasks")
def dispatch(req:TaskRequest):
    global rr
    with rrlock: target=workers[rr%len(workers)]; rr+=1
    try:
        r=requests.post(target+"/task",json=req.model_dump(),timeout=20); r.raise_for_status()
        return r.json()
    except requests.RequestException as e: return {"status":"failed","error":str(e)}

@app.post("/coordination/election")
def election():
    participants=["controller"]
    for w in workers:
        try: participants.append(requests.get(w+"/health",timeout=1).json()["node"])
        except requests.RequestException: pass
    return {"algorithm":"deterministic leader-election demonstration",
            "participants":participants,"leader":sorted(participants)[0],
            "coordination_message_count":len(participants)-1}

@app.post("/coordination/event")
def coordination_event(payload:dict):
    start=time.perf_counter(); c=tick(); acks=[]
    for w in workers:
        try: acks.append(requests.post(w+"/clock/event",
                    json={"event":payload.get("event"),"clock":c},timeout=2).json())
        except requests.RequestException: pass
    return {"event":payload.get("event"),"logical_clock":c,
            "acknowledgements":acks,
            "synchronization_delay_seconds":time.perf_counter()-start}

if __name__=="__main__": uvicorn.run(app,host="127.0.0.1",port=PORT,log_level="warning")
