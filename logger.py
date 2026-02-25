import os
import time
import requests
import json
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# --- Config ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_FILE = os.path.join(BASE_DIR, "uploader_sql.log")

LOKI_URL = os.getenv("LOKI_URL")
LOKI_USER_ID = os.getenv("LOKI_USER_ID")
LOKI_API_TOKEN = os.getenv("LOKI_API_TOKEN")
DEVICE_NAME = os.getenv("DEVICE_NAME", "Unknown_Device")
SERVICE_NAME = os.getenv("SERVICE_NAME", "Unknown_Service")

if not LOKI_URL or not LOKI_USER_ID or not LOKI_API_TOKEN:
    print("❌ Missing Loki configuration in .env. Please add LOKI_URL, LOKI_USER_ID, and LOKI_API_TOKEN.")
    exit(1)

# Ensure the URL has the correct endpoint path
LOKI_PUSH_URL = LOKI_URL.rstrip("/") + "/loki/api/v1/push"

def push_to_loki(log_line):
    """Pushes a single log line to the Loki server."""
    # Loki expects Unix timestamp in nanoseconds as a string
    timestamp_ns = str(int(time.time() * 1e9))
    
    payload = {
        "streams": [
            {
                "stream": {
                    "service_name": SERVICE_NAME,
                    "device": DEVICE_NAME
                },
                "values": [
                    [timestamp_ns, log_line.strip()]
                ]
            }
        ]
    }

    try:
        response = requests.post(
            LOKI_PUSH_URL,
            auth=(LOKI_USER_ID, LOKI_API_TOKEN),
            headers={"Content-type": "application/json"},
            json=payload,
            timeout=5
        )
        
        if response.status_code != 204:
            print(f"⚠️ Failed to push log. Status: {response.status_code}, Response: {response.text}")
            
    except Exception as e:
        print(f"⚠️ Loki connection error: {e}")

def watch_log_file(file_path):
    """Tails the log file continuously and pushes new lines to Loki."""
    if not os.path.exists(file_path):
        print(f"⏳ Waiting for {file_path} to be created...")
        while not os.path.exists(file_path):
            time.sleep(1)
            
    print(f"👀 Watching {file_path} for new logs...")
    
    with open(file_path, "r", encoding="utf-8") as file:
        # Seek to the end of the file to only read new logs
        file.seek(0, 2)
        
        while True:
            line = file.readline()
            if not line:
                time.sleep(0.5) # Wait briefly before checking again
                continue
                
            push_to_loki(line)

if __name__ == "__main__":
    try:
        watch_log_file(LOG_FILE)
    except KeyboardInterrupt:
        print("\n🛑 Stopped watching logs.")
