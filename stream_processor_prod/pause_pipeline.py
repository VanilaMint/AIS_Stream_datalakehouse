import requests
import time

FLINK_API = "http://jobmanager:8081/jobs"
SAVEPOINT_DIR = "/opt/flink/data/savepoints" 

def pause_cluster():
    print("Fetching running jobs...")
    response = requests.get(FLINK_API).json()
    
    running_jobs = [job for job in response['jobs'] if job['status'] == 'RUNNING']
    
    if not running_jobs:
        print("No running jobs found.")
        return

    for job in running_jobs:
        job_id = job['id']
        

        job_details = requests.get(f"{FLINK_API}/{job_id}").json()
        
        job_name = job_details.get('name')
        
        target_dir = f"{SAVEPOINT_DIR}/{job_name}"
        
        print(f"Stopping '{job_name}' and saving state to {target_dir}...")
        
        payload = {
            "targetDirectory": target_dir,
            "drain": False
        }
        stop_res = requests.post(f"{FLINK_API}/{job_id}/stop", json=payload)
        
        if stop_res.status_code == 202:
            request_id = stop_res.json()['request-id']
            _wait_for_savepoint(job_id, request_id)
        else:
            print(f"Failed to stop {job_name}: {stop_res.text}")

def _wait_for_savepoint(job_id, request_id):
    """Poll the API until the savepoint finishes writing to disk"""
    status_url = f"{FLINK_API}/{job_id}/savepoints/{request_id}"
    while True:
        res = requests.get(status_url).json()
        status = res['status']['id']
        if status == 'COMPLETED':
            print("✅ Savepoint completed successfully!\n")
            break
        elif status == 'FAILED':
            print("❌ Savepoint failed!\n")
            break
        time.sleep(1)

if __name__ == '__main__':
    pause_cluster()
    print("All jobs paused. You can now safely shut down Docker!")