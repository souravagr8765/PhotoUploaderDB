import os
import time
import requests
import json
from datetime import datetime
from dotenv import load_dotenv
import logging

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

# Ensure the URL has the correct endpoint path
LOKI_PUSH_URL = ""
if LOKI_URL:
    LOKI_PUSH_URL = LOKI_URL.rstrip("/") + "/loki/api/v1/push"

# Configure a module-level standard logger for local fallback (if needed)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler()
    ]
)

_internal_logger = logging.getLogger("lg")

import threading
import queue
import atexit

# Queue for background log pushing
log_queue = queue.Queue()
_exit_event = threading.Event()

def _loki_worker():
    while not _exit_event.is_set():
        # Wait up to 5 seconds to batch logs, or until exit is signaled
        _exit_event.wait(5.0)
        
        batch = []
        while True:
            try:
                # Pluck all available items from the queue
                log_item = log_queue.get_nowait()
                if log_item is None:
                    log_queue.task_done()
                    continue
                batch.append(log_item)
                log_queue.task_done()
            except queue.Empty:
                break
                
        if batch:
            # Loki requires logs to be strictly in chronological order per stream
            batch.sort(key=lambda x: int(x[0]))
            
            # Send in chunks of 1000 to prevent payload size issues
            chunk_size = 1000
            for i in range(0, len(batch), chunk_size):
                chunk = batch[i:i+chunk_size]
                try:
                    _push_batch_to_loki(chunk)
                except Exception as e:
                    print(f"⚠️ Failsafe batch push error: {e}")

# Start background thread
_worker_thread = threading.Thread(target=_loki_worker, daemon=True)
_worker_thread.start()

def _cleanup_logger():
    # Signal the thread to wake up and process the final batch immediately
    _exit_event.set()
    log_queue.put(None) # Give queue a prod just in case
    _worker_thread.join(timeout=5.0)

atexit.register(_cleanup_logger)

def _format_and_push(level: str, msg: str, *args):
    # Format the message like traditional logging if args exist
    if args:
        try:
            formatted_msg = msg % args
        except Exception:
            formatted_msg = msg + " " + str(args)
    else:
        formatted_msg = str(msg)
        
    full_log = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]} [{level}] {formatted_msg}"
    
    # Still log to console for visibility
    if level == "INFO": _internal_logger.info(formatted_msg)
    elif level == "WARNING": _internal_logger.warning(formatted_msg)
    elif level == "ERROR": _internal_logger.error(formatted_msg)
    elif level == "CRITICAL": _internal_logger.critical(formatted_msg)
    elif level == "DEBUG": _internal_logger.debug(formatted_msg)
    
    # Push the formatted string to background queue
    timestamp_ns = str(time.time_ns())
    log_queue.put((timestamp_ns, full_log))

def info(msg, *args, **kwargs):
    _format_and_push("INFO", msg, *args)

def warning(msg, *args, **kwargs):
    _format_and_push("WARNING", msg, *args)

def error(msg, *args, **kwargs):
    _format_and_push("ERROR", msg, *args)

def critical(msg, *args, **kwargs):
    _format_and_push("CRITICAL", msg, *args)

def debug(msg, *args, **kwargs):
    _format_and_push("DEBUG", msg, *args)

def push_to_loki(log_line):
    # Backward compatibility if anything calls this directly
    timestamp_ns = str(time.time_ns())
    log_queue.put((timestamp_ns, log_line))

def _push_batch_to_loki(batch):
    """Pushes a batch of log lines to the Loki server."""
    if not LOKI_PUSH_URL:
        return
        
    values = []
    last_ts = 0
    for timestamp_ns, log_line in batch:
        ts = int(timestamp_ns)
        # Ensure timestamps are strictly increasing by at least 1 ns
        if ts <= last_ts:
            ts = last_ts + 1
        last_ts = ts
        values.append([str(ts), log_line.strip()])
        
    payload = {
        "streams": [
            {
                "stream": {
                    "service_name": SERVICE_NAME,
                    "device": DEVICE_NAME
                },
                "values": values
            }
        ]
    }

    try:
        response = requests.post(
            LOKI_PUSH_URL,
            auth=(LOKI_USER_ID, LOKI_API_TOKEN),
            headers={"Content-type": "application/json"},
            json=payload,
            timeout=10
        )
        
        if response.status_code != 204:
            print(f"⚠️ Failed to push log batch. Status: {response.status_code}, Response: {response.text}")
            
    except Exception as e:
        print(f"⚠️ Loki connection error during batch push: {e}")

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
