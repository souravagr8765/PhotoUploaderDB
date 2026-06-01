import os
import json
import pickle
import subprocess
import socket
import time
import requests
from google.auth.transport.requests import Request
from infra.config_loader import get_config
import infra.logger as logger
from infra.notifications import send_notification

# Global Config references
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "Data")
ACTIVE_ACC_FILE = os.path.join(DATA_DIR, "active_account.txt")

ACCOUNTS = get_config("app.google_accounts", [])
if not ACCOUNTS:
    import infra.logger as _startup_logger
    _startup_logger.error("❌ google_accounts is not set in config.yaml. Please add a list of Google account emails.")

def wait_for_internet(timeout=5, retry_interval=10):
    first_attempt = True
    while True:
        try:
            socket.create_connection(("8.8.8.8", 53), timeout=timeout)
            if not first_attempt:
                logger.info("🌐 Internet Connection Restored.")
            return
        except OSError:
            if first_attempt:
                logger.warning("🌐 No internet. Waiting...")
                first_attempt = False
            time.sleep(retry_interval)

def get_storage_usage(remote, creds=None):
    """
    Checks storage usage.
    If creds are provided, it uses the Google Drive API directly (faster).
    Otherwise, it falls back to rclone (slower).
    Returns (used_bytes, total_bytes).
    """
    wait_for_internet()

    # Fast Path: Use direct API if tokens are available
    if creds:
        try:
            headers = {'Authorization': f'Bearer {creds.token}'}
            resp = requests.get(
                'https://www.googleapis.com/drive/v3/about?fields=storageQuota',
                headers=headers, timeout=10
            )
            if resp.status_code == 200:
                data = resp.json().get('storageQuota', {})
                limit = int(data.get('limit', 1))
                usage = int(data.get('usage', 0))
                return usage, limit
        except Exception as e:
            logger.debug(f"Direct API storage check failed: {e}")

    # Fallback Path: Rclone (Original behavior)
    try:
        result = subprocess.run(['rclone', 'about', f'{remote}:', '--json'], capture_output=True, text=True, shell=False, check=True)
        data = json.loads(result.stdout)
        used = data.get("used", 0) + data.get("other", 0)
        total = data.get("total", 1)
        return used, total
    except Exception:
        return 0, 1
def get_account_info(idx):
    if idx < 0 or idx >= len(ACCOUNTS):
        return None, None
    email = ACCOUNTS[idx]
    remote = "gdrive" + email.replace("@gmail.com", "")
    return email, remote

def get_active_account_info():
    idx = 0
    if os.path.exists(ACTIVE_ACC_FILE):
        with open(ACTIVE_ACC_FILE, 'r') as f:
            try: idx = int(f.read().strip())
            except: idx = 0
    
    if idx >= len(ACCOUNTS): idx = len(ACCOUNTS) - 1
    email, remote = get_account_info(idx)
    return email, remote, idx

def switch_account(current_idx, current_email, usage_percent, albums_cache, device_name):
    """Permanently switches the active account in ACTIVE_ACC_FILE."""
    next_idx = current_idx + 1
    if next_idx < len(ACCOUNTS):
        next_email = ACCOUNTS[next_idx]
        
        # 1. Alert Notification
        subject = f"⚠️ Storage Full: {current_email}"
        body = (f"Account {current_email} is full ({usage_percent:.2f}%).\n"
                f"Moving default account to: {next_email}\n"
                f"Device: {device_name}")
        send_notification(subject, body)
        
        # 2. Update File
        with open(ACTIVE_ACC_FILE, 'w') as f: f.write(str(next_idx))
        
        # NOTE: We no longer clear the entire albums_cache here because 
        # the system now supports best-fit account selection and account-specific caches.
        
        return True # Switched
    else:
        logger.error("❌ No more accounts available!")
        return False

def get_creds(email):
    token_path = os.path.join(BASE_DIR, "creds", f"token_{email}.pkl")
    if os.path.exists(token_path):
        with open(token_path, "rb") as f:
            creds = pickle.load(f)
            if creds and creds.expired and creds.refresh_token:
                try:
                    # Request refresh with the same scopes we now need
                    creds.refresh(Request())
                    with open(token_path, "wb") as f_out: 
                        pickle.dump(creds, f_out)
                except Exception as e:
                    logger.error(f"Failed to refresh token: {e}")
                    return None
            return creds
    return None
