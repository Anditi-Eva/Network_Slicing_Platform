import requests
import json
import time

# UPDATE THIS IP: Put your Master Controller's actual IP address here
MASTER_IP = "192.168.2.113/24"  
BASE_URL = f"http://{MASTER_IP}:5000/api/slices"

def get_status():
    print("\n--- Current Cluster Status ---")
    try:
        res = requests.get(BASE_URL)
        print(json.dumps(res.json(), indent=2))
    except Exception as e:
        print(f"Error connecting to Master Controller: {e}")

def create_slice(slice_id, slice_type, bandwidth, latency):
    print(f"\n--- Requesting Slice Creation: {slice_id} ---")
    payload = {
        "slice_id": slice_id,
        "slice_type": slice_type,
        "bandwidth": bandwidth,
        "latency": latency
    }
    try:
        res = requests.post(BASE_URL, json=payload)
        print(f"Response ({res.status_code}): {res.json()}")
    except Exception as e:
        print(f"Error: {e}")

def delete_slice(slice_id):
    print(f"\n--- Deleting Slice: {slice_id} ---")
    try:
        res = requests.delete(f"{BASE_URL}/{slice_id}")
        print(f"Response ({res.status_code}): {res.json()}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    print("[CLIENT] Initializing Client Simulation...")
    
    # 1. Fetch initial status
    get_status()

    # 2. Request an eMBB (Broadband) Slice
    create_slice("slice_embb_video", "eMBB", 300, 15)

    # 3. Request a URLLC (Low-Latency) Slice
    create_slice("slice_urllc_telemed", "URLLC", 100, 5)

    # 4. View updated status showing worker assignments
    time.sleep(1)
    get_status()

    # 5. Tear down a slice
    delete_slice("slice_embb_video")
    
    # 6. Final verification
    time.sleep(1)
    get_status()