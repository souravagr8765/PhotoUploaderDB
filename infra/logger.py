import os
import time
import requests
import json
from datetime import datetime
from infra.config_loader import get_config
import logging
import inspect

# Log file goes to project root, not inside infra/
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG_FILE = os.path.join(BASE_DIR, "uploader_sql.log")

LOKI_URL = get_config("logging.loki_url")
LOKI_USER_ID = get_config("logging.loki_user_id")
LOKI_API_TOKEN = get_config("logging.loki_api_token")
DEVICE_NAME = get_config("app.device_name", "Unknown_Device")
SERVICE_NAME = get_config("app.service_name", "Unknown_Service")

if not LOKI_URL or not LOKI_USER_ID or not LOKI_API_TOKEN:
    print("❌ Missing Loki configuration in config.yaml. Please add logging.loki_url, logging.loki_user_id, and logging.loki_api_token.")

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

def _get_caller_name():
    """Automatically detects the name of the module that called the logger."""
    try:
        # frame 0: _get_caller_name
        # frame 1: _format_and_push
        # frame 2: info/warning/etc.
        # frame 3: the actual caller
        frame = inspect.currentframe()
        for _ in range(3):
            if frame:
                frame = frame.f_back
        if frame:
            # Try to get the full module name (e.g., 'core.scanner')
            module = inspect.getmodule(frame)
            if module and module.__name__ != "__main__":
                return module.__name__
            
            # Fallback: Get the filename and return the base name without extension
            # This is useful for scripts run directly (where __name__ == "__main__")
            return os.path.splitext(os.path.basename(frame.f_code.co_filename))[0]
    except Exception:
        pass
    return "unknown"

def _format_and_push(level: str, msg: str, *args, module: str = None):
    # Format the message like traditional logging if args exist
    if args:
        try:
            formatted_msg = msg % args
        except Exception:
            formatted_msg = msg + " " + str(args)
    else:
        formatted_msg = str(msg)

    if module is None:
        module = _get_caller_name()

    # Capture the current time ONCE — this is the single source of truth
    now = datetime.now()

    # Still log to console/file for visibility (the internal logger adds its own timestamp)
    # Prepend the module name to the local log message
    local_msg = f"[{module}] {formatted_msg}"
    if level == "INFO": _internal_logger.info(local_msg)
    elif level == "WARNING": _internal_logger.warning(local_msg)
    elif level == "ERROR": _internal_logger.error(local_msg)
    elif level == "CRITICAL": _internal_logger.critical(local_msg)
    elif level == "DEBUG": _internal_logger.debug(local_msg)

    # Use the application's datetime as Loki's timestamp (not upload time).
    # Level and Module are sent as Loki labels.
    timestamp_ns = str(int(now.timestamp() * 1e9))
    log_queue.put((timestamp_ns, level.lower(), module, formatted_msg))

def info(msg, *args, **kwargs):
    _format_and_push("INFO", msg, *args, module=kwargs.get("module"))

def warning(msg, *args, **kwargs):
    _format_and_push("WARNING", msg, *args, module=kwargs.get("module"))

def error(msg, *args, **kwargs):
    _format_and_push("ERROR", msg, *args, module=kwargs.get("module"))

def critical(msg, *args, **kwargs):
    _format_and_push("CRITICAL", msg, *args, module=kwargs.get("module"))

def debug(msg, *args, **kwargs):
    _format_and_push("DEBUG", msg, *args, module=kwargs.get("module"))

def push_to_loki(log_line, module="legacy"):
    # Backward compatibility if anything calls this directly
    timestamp_ns = str(time.time_ns())
    log_queue.put((timestamp_ns, "info", module, log_line))

class ModuleLogger:
    """Explicit logger for a specific module."""
    def __init__(self, name):
        self.name = name
    def info(self, msg, *args): _format_and_push("INFO", msg, *args, module=self.name)
    def warning(self, msg, *args): _format_and_push("WARNING", msg, *args, module=self.name)
    def error(self, msg, *args): _format_and_push("ERROR", msg, *args, module=self.name)
    def critical(self, msg, *args): _format_and_push("CRITICAL", msg, *args, module=self.name)
    def debug(self, msg, *args): _format_and_push("DEBUG", msg, *args, module=self.name)

def get_logger(name):
    """Returns a ModuleLogger instance with a fixed module name."""
    return ModuleLogger(name)

def _push_batch_to_loki(batch):
    """Pushes a batch of log lines to the Loki server, grouped by level and module."""
    if not LOKI_PUSH_URL:
        return

    # Group log entries by (level, module) so each combo gets its own Loki stream
    from collections import defaultdict
    stream_groups = defaultdict(list)
    stream_last_ts = defaultdict(int)

    for timestamp_ns, level, module, log_line in batch:
        ts = int(timestamp_ns)
        stream_key = (level, module)
        
        # Ensure timestamps are strictly increasing per stream
        if ts <= stream_last_ts[stream_key]:
            ts = stream_last_ts[stream_key] + 1
        stream_last_ts[stream_key] = ts
        
        stream_groups[stream_key].append([str(ts), log_line.strip()])

    streams = []
    for (level, module), values in stream_groups.items():
        streams.append({
            "stream": {
                "service_name": SERVICE_NAME,
                "device": DEVICE_NAME,
                "level": level,
                "module": module
            },
            "values": values
        })

    payload = {"streams": streams}

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
