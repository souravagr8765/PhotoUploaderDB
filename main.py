#!/usr/bin/env python3
import os
import json
import subprocess
import requests
import pickle
import time
import logging
import smtplib
import socket
import shutil
from datetime import datetime
from email.message import EmailMessage
from google.auth.transport.requests import Request

# ===== GLOBAL CONFIG =====
DEVICE_NAME = "OPPO_R13"         
ACCOUNTS = ["souravagarwalfarewell@gmail.com", "ca.aspirant.sourav.agarwal@gmail.com","photouploader.sourav@gmail.com","photouploader.souravagarwal@gmail.com"] 
SENDER_EMAIL = "srvagr8765@gmail.com"
RECEIVER_EMAIL = "photouploader.sourav@gmail.com"
APP_PASSWORD = "vryz wrxt lvak ipnr"

# --- MULTI-FOLDER LIST ---
SOURCE_DIRECTORIES = [
    "/storage/emulated/0/DCIM/Camera",
    "/sdcard/Android/media/com.whatsapp/WhatsApp/Media/WhatsApp Images",
    "/sdcard/Android/media/com.whatsapp/WhatsApp/Media/WhatsApp Video",
    "/sdcard/Android/media/com.whatsapp/WhatsApp/Media/WhatsApp Documents",
    "/storage/emulated/0/Pictures/Screenshots",
]

# Path Config
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "Data")
ALL_INDICES_DIR = os.path.join(DATA_DIR, "all_indices")
MY_BACKUP_DIR = os.path.join(DATA_DIR, "Backups", DEVICE_NAME)
HISTORY_DIR = os.path.join(BASE_DIR, "UploadHistory")
ACTIVE_ACC_FILE = os.path.join(DATA_DIR, "active_account.txt")

# Rclone Config
INDEX_REMOTE = "gdrivesrvagr8765"
REMOTE_DATA_DIR = f"{INDEX_REMOTE}:backups/Data"
MY_REMOTE_INDEX = f"{REMOTE_DATA_DIR}/all_indices/index_{DEVICE_NAME}.json"

LOGFILE = os.path.join(BASE_DIR, "uploader.log")
VALID_EXTENSIONS = ('.jpg', '.jpeg', '.png', '.gif', '.bmp', '.heic',
                    '.mp4', '.mov', '.avi', '.mkv', '.webm')

# Ensure directories exist
os.makedirs(ALL_INDICES_DIR, exist_ok=True)
os.makedirs(MY_BACKUP_DIR, exist_ok=True)
os.makedirs(HISTORY_DIR, exist_ok=True)

# Setup logging
logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(LOGFILE, encoding="utf-8"), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# ===== Utility & Connection Functions =====

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

def get_storage_usage(remote):
    wait_for_internet()
    result = subprocess.run(f'rclone about "{remote}:" --json', capture_output=True, text=True, shell=True)
    if result.returncode == 0:
        try:
            data = json.loads(result.stdout)
            used = data.get("used", 0) + data.get("other", 0)
            total = data.get("total", 1)
            return (used / total) * 100
        except: return 0
    return 0

def get_active_account_info():
    idx = 0
    if os.path.exists(ACTIVE_ACC_FILE):
        with open(ACTIVE_ACC_FILE, 'r') as f:
            try: idx = int(f.read().strip())
            except: idx = 0
    
    if idx >= len(ACCOUNTS): idx = len(ACCOUNTS) - 1
    email = ACCOUNTS[idx]
    remote = "gdrive" + email.replace("@gmail.com", "")
    return email, remote, idx

def get_creds(email):
    token_path = os.path.join(BASE_DIR, f"token_{email}.pkl")
    if os.path.exists(token_path):
        with open(token_path, "rb") as f:
            creds = pickle.load(f)
            if creds and creds.expired and creds.refresh_token:
                try:
                    creds.refresh(Request())
                    with open(token_path, "wb") as f: pickle.dump(creds, f)
                except: return None
            return creds
    return None

# ===== Core Operations =====

def upload_file(creds, path):
    wait_for_internet()
    filename = os.path.basename(path)
    headers = {'Authorization': f'Bearer {creds.token}', 'Content-type': 'application/octet-stream', 'X-Goog-Upload-File-Name': filename, 'X-Goog-Upload-Protocol': 'raw'}
    try:
        with open(path, 'rb') as f:
            resp = requests.post('https://photoslibrary.googleapis.com/v1/uploads', data=f, headers=headers)
        if resp.status_code == 200:
            upload_token = resp.text
            create_resp = requests.post(
                'https://photoslibrary.googleapis.com/v1/mediaItems:batchCreate',
                headers={'Authorization': f'Bearer {creds.token}', 'Content-type': 'application/json'},
                json={"newMediaItems": [{"simpleMediaItem": {"uploadToken": upload_token}}]}
            )
            return create_resp.status_code == 200
    except: return False
    return False

def archive_and_prune():
    my_local_index = os.path.join(ALL_INDICES_DIR, f"index_{DEVICE_NAME}.json")
    if not os.path.exists(my_local_index): return
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = os.path.join(MY_BACKUP_DIR, f"index_{timestamp}.json")
    shutil.copy2(my_local_index, backup_path)
    backups = sorted([os.path.join(MY_BACKUP_DIR, f) for f in os.listdir(MY_BACKUP_DIR) if f.endswith('.json')], key=os.path.getmtime)
    while len(backups) > 30:
        os.remove(backups.pop(0))

def get_global_library_size():
    total_bytes = 0
    for entry in os.scandir(ALL_INDICES_DIR):
        if entry.name.endswith(".json"):
            try:
                with open(entry.path, 'r') as f:
                    data = json.load(f)
                    total_bytes += sum(item.get('size', 0) for item in data)
            except: continue
    return total_bytes

# ===== Main Logic =====

def main():
    # 1. Start-up: Sync all indices to know what's already uploaded globally
    wait_for_internet()
    subprocess.run(f'rclone sync "{REMOTE_DATA_DIR}/all_indices" "{ALL_INDICES_DIR}"', shell=True, capture_output=True)
    
    master_uploaded_set = set()
    for entry in os.scandir(ALL_INDICES_DIR):
        if entry.name.endswith(".json"):
            try:
                with open(entry.path, 'r') as f:
                    for item in json.load(f): master_uploaded_set.add(item['filename'].lower())
            except: continue

    # 2. Setup Current Account
    email, remote, acc_idx = get_active_account_info()
    creds = get_creds(email)
    if not creds:
        logger.error(f"❌ Authentication failed for {email}. Ensure token_{email}.pkl exists.")
        return

    # 3. Load Device Index
    my_index_path = os.path.join(ALL_INDICES_DIR, f"index_{DEVICE_NAME}.json")
    try:
        if os.path.exists(my_index_path):
            with open(my_index_path, 'r') as f: my_index_data = json.load(f)
        else: my_index_data = []
    except: my_index_data = []

    session_files, session_bytes = [], 0
    start_time = datetime.now()

    # 4. Scanning all Folders
    for folder in SOURCE_DIRECTORIES:
        if not os.path.exists(folder):
            logger.warning(f"Skipping missing folder: {folder}")
            continue
            
        logger.info(f"📂 Scanning: {folder}")
        for root, _, files in os.walk(folder):
            for file in files:
                if file.lower().endswith(VALID_EXTENSIONS) and file.lower() not in master_uploaded_set:
                    
                    # Storage Check
                    usage = get_storage_usage(remote)
                    if usage >= 90:
                        logger.warning(f"⚠️ Account {email} is full ({usage:.2f}%).")
                        if acc_idx + 1 < len(ACCOUNTS):
                            with open(ACTIVE_ACC_FILE, 'w') as f: f.write(str(acc_idx + 1))
                            logger.info("🔄 Switching to next account...")
                            return main() # Restart with new account
                        else:
                            logger.error("❌ No more accounts available. stopping.")
                            break

                    filepath = os.path.join(root, file)
                    f_size = os.path.getsize(filepath)
                    
                    logger.info(f"🚀 Uploading: {file}")
                    if upload_file(creds, filepath):
                        logger.info(f"✅ Success: {file}")
                        my_index_data.append({"filename": file, "account": email, "size": f_size})
                        session_bytes += f_size
                        session_files.append(file)
                        master_uploaded_set.add(file.lower())
                        
                        # Save progress locally immediately
                        with open(my_index_path, 'w') as f: json.dump(my_index_data, f, indent=2)

    # 5. Finalize Session
    if session_files:
        end_time = datetime.now()
        # Create History text file
        list_path = os.path.join(HISTORY_DIR, f"{end_time.strftime('%Y%m%d_%H%M%S')}.txt")
        with open(list_path, 'w') as f: f.write("\n".join(session_files))
        
        # Archive local index & Clean old backups
        archive_and_prune()

        # Cloud Sync: Sync WHOLE Data folder and target device index
        logger.info("📤 Uploading local state to cloud...")
        subprocess.run(f'rclone copyto "{my_index_path}" "{MY_REMOTE_INDEX}"', shell=True)
        subprocess.run(f'rclone sync "{DATA_DIR}" "{REMOTE_DATA_DIR}"', shell=True)
        
        # Calculate Global Stats
        global_total = get_global_library_size()
        logger.info(f"✅ Finished. Global Library Size: {global_total / (1024**3):.4f} GB")
        # (Optional: call send_report here)
    else:
        logger.info("No new files found across all directories.")

if __name__ == "__main__":
    main()