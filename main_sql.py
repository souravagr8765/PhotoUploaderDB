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
import hashlib
from datetime import datetime
from email.message import EmailMessage
from google.auth.transport.requests import Request
from dotenv import load_dotenv

# Import our new Database Manager
from database import DatabaseManager
from metadata_engine import get_assigned_album
load_dotenv()

# ===== GLOBAL CONFIG =====
DEVICE_NAME = os.getenv("DEVICE_NAME", "Unknown_Device")
ACCOUNTS = ["souravagarwalchildrensday@gmail.com", "ca.aspirant.sourav.agarwal@gmail.com","photouploader.sourav@gmail.com","photouploader.souravagarwal@gmail.com"] 
SENDER_EMAIL = os.getenv("SENDER_EMAIL")
RECEIVER_EMAIL = os.getenv("RECEIVER_EMAIL")
APP_PASSWORD = os.getenv("APP_PASSWORD")

# --- MULTI-FOLDER LIST ---
SOURCE_DIRECTORIES = [
   "E:\\FamilyMemories\\library\\admin"
]

# Path Config
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "Data")
HISTORY_DIR = os.path.join(BASE_DIR, "UploadHistory")
ACTIVE_ACC_FILE = os.path.join(DATA_DIR, "active_account.txt")
BACKUP_DB_PATH = os.path.join(DATA_DIR, "Backups", f"backup_{DEVICE_NAME}.db")
LOGFILE = os.path.join(BASE_DIR, "uploader_sql.log")

VALID_EXTENSIONS = ('.jpg', '.jpeg', '.png', '.gif', '.bmp', '.heic',
                    '.mp4', '.mov', '.avi', '.mkv', '.webm')

# Ensure directories exist
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(os.path.dirname(BACKUP_DB_PATH), exist_ok=True)
os.makedirs(HISTORY_DIR, exist_ok=True)

# Setup logging
logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(LOGFILE, encoding="utf-8"), logging.StreamHandler()]
)
logger = logging.getLogger("Main")

# Runtime Cache for Albums { "Album Name": "album_id" }
ALBUMS_CACHE = {}

# ===== Utilities =====

def calculate_file_hash(filepath: str) -> str:
    """Calculates SHA-256 hash of a file."""
    sha256_hash = hashlib.sha256()
    with open(filepath, "rb") as f:
        # Read in chunks for large files
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()

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

def send_email(subject, body):
    if not SENDER_EMAIL or not APP_PASSWORD:
        logger.warning("⚠️ Email not configured. Skipping email.")
        return

    msg = EmailMessage()
    msg.set_content(body)
    msg['Subject'] = subject
    msg['From'] = SENDER_EMAIL
    msg['To'] = RECEIVER_EMAIL

    try:
        server = smtplib.SMTP_SSL("smtp.gmail.com", 465)
        server.login(SENDER_EMAIL, APP_PASSWORD)
        server.send_message(msg)
        server.quit()
        logger.info(f"📧 Email sent: {subject}")
    except Exception as e:
        logger.error(f"❌ Failed to send email: {e}")

# ===== Account & Storage Management =====

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

def switch_account(current_idx, current_email, usage_percent):
    next_idx = current_idx + 1
    if next_idx < len(ACCOUNTS):
        next_email = ACCOUNTS[next_idx]
        
        # 1. Alert Email
        subject = f"⚠️ Storage Full: {current_email}"
        body = (f"Account {current_email} is full ({usage_percent:.2f}%).\n"
                f"Switching to next account: {next_email}\n"
                f"Device: {DEVICE_NAME}")
        send_email(subject, body)
        
        # 2. Update File
        with open(ACTIVE_ACC_FILE, 'w') as f: f.write(str(next_idx))
        
        # Clear Album Cache on switch (new account = empty albums)
        ALBUMS_CACHE.clear()
        
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
                    creds.refresh(Request())
                    with open(token_path, "wb") as f: pickle.dump(creds, f)
                except: return None
            return creds
    return None

# ===== Upload Logic =====
def get_or_create_album(creds, album_name, db, email, saved_album_id=None):
    """
    Checks if album exists (via cache or API). If not, creates it.
    Returns: (album_id_for_upload, updated_db_album_id_string)
    """
    if not album_name: return None, None
    
    album_dict = None
    
    # 0. Check Saved JSON ID vs Legacy ID
    if saved_album_id:
        is_multi = False
        album_dict = {}
        if saved_album_id.strip().startswith("{") and saved_album_id.strip().endswith("}"):
            try:
                album_dict = json.loads(saved_album_id)
                is_multi = True
            except: pass
            
        if is_multi:
            if email in album_dict:
                # We already have an ID for this specific account
                ALBUMS_CACHE[album_name] = album_dict[email]
                return album_dict[email], saved_album_id
            # If we don't have it, we fall through to create it below
        else:
            # Legacy Single ID (not JSON)
            # If it's a legacy ID, it belongs to whoever created the trip first.
            # Usually the first account. We treat it as unknown creator unless it's the current session.
            # But let's build a dict moving forward
            album_dict = {"legacy_creator": saved_album_id}
            
            # If it's the first account, it's highly likely this legacy ID belongs to them
            # We'll just assume they own it unless proven otherwise, or we can just try to upload.
            # To be safe, let's just use it if it's the very first account, else create new.
            # If this is the active account that made it, we might just be reusing the legacy.
            # Since we can't definitively know without `albums.get`, we'll let Google give a 404 on upload if wrong, 
            # OR we just migrate it. We'll simply migrate and try it.
            
            # Simple assumption: let's just try the legacy ID. If it fails downstream, it fails.
            # But if a new account switches, they need a NEW id. The best way is to assume legacy 
            # belongs to the FIRST account `ACCOUNTS[0]`.
            if email == ACCOUNTS[0]:
                ALBUMS_CACHE[album_name] = saved_album_id
                return saved_album_id, saved_album_id
            
            # Otherwise, fall through to create a new one for this non-default account.

    # 1. Check Runtime Cache
    if album_name in ALBUMS_CACHE:
        # Returning None for db_id means no change needed to DB
        return ALBUMS_CACHE[album_name], None
        
    wait_for_internet()
    
    # 2. Search API (List albums and check name)
    # Note: Google Photos API list is paginated. For simplicity, we create if we haven't seen it in this session 
    # OR we could blindly try create? No, duplicate names are allowed in GPhotos, which is messy.
    # Correct way: List all albums once at startup? Too slow.
    # Strategy: Try to create. If name exists? Google Photos ALLOWS duplicate names.
    # To prevent duplicates, we really should search.
    # FAST PATH: Just create it and cache it. If run multiple times, might create duplicates.
    # BETTER PATH: List albums matching title? API doesn't support filter by title easily.
    
    # Let's simple-create for now and cache for the session.
    # IMPROVEMENT: Load existing albums at startup?
    
    headers = {
        'Authorization': f'Bearer {creds.token}', 
        'Content-type': 'application/json'
    }
    
    try:
        # Create Album
        payload = {"album": {"title": album_name}}
        resp = requests.post('https://photoslibrary.googleapis.com/v1/albums', headers=headers, json=payload)
        
        if resp.status_code == 200:
            data = resp.json()
            album_id = data.get("id")
            ALBUMS_CACHE[album_name] = album_id
            
            # Add to dictionary
            if album_dict is None:
                album_dict = {}
            album_dict[email] = album_id
            
            new_saved_id = json.dumps(album_dict)
            
            # Save to persistent database
            db.update_trip_album_id(album_name, new_saved_id)
            
            logger.info(f"📁 Created Album '{album_name}' for account {email}")
            
            # Check if this is a secondary album creation (i.e. album_dict has > 1 key)
            if len(album_dict) > 1:
                subject = f"🔔 Album Split Notification: {album_name}"
                body = (f"Storage was full, so a NEW part of the album '{album_name}' "
                        f"was created on account: {email}.\n\n"
                        f"IMPORTANT: Please open Google Photos for {email} and manually share "
                        f"this album with your main account to merge them together!")
                send_email(subject, body)
            
            return album_id, new_saved_id
        else:
            logger.error(f"Failed to create album {album_name}: {resp.text}")
            return None, None
    except Exception as e:
        logger.error(f"Album API Error: {e}")
        return None, None


def upload_file_to_google(creds, path, album_id=None):
    wait_for_internet()
    filename = os.path.basename(path)
    headers = {
        'Authorization': f'Bearer {creds.token}', 
        'Content-type': 'application/octet-stream', 
        'X-Goog-Upload-File-Name': filename, 
        'X-Goog-Upload-Protocol': 'raw'
    }
    try:
        with open(path, 'rb') as f:
            resp = requests.post('https://photoslibrary.googleapis.com/v1/uploads', data=f, headers=headers)
        
        if resp.status_code == 200:
            upload_token = resp.text
            
            # Create Media Item
            body = {"newMediaItems": [{"simpleMediaItem": {"uploadToken": upload_token}}]}
            
            # Add to Album if specified
            if album_id:
                body["albumId"] = album_id
                
            create_resp = requests.post(
                'https://photoslibrary.googleapis.com/v1/mediaItems:batchCreate',
                headers={'Authorization': f'Bearer {creds.token}', 'Content-type': 'application/json'},
                json=body
            )
            # Check if any items were successfully created
            if create_resp.status_code == 200:
                res_json = create_resp.json()
                # Check specifics if needed, but for now allow generic success
                return True, res_json
    except Exception as e:
        logger.error(f"Upload API Error: {e}")
        return False, None
    return False, None

# ===== Main Loop =====

def main():
    logger.info("🚀 Starting Photo Uploader (SQL Edition)...")
    start_time = datetime.now()
    
    # 1. Init Database
    try:
        db = DatabaseManager(use_local_cache=True)
        if not db.check_connection():
            logger.error("Failed to connect to Supabase. Exiting.")
            return
        
        # Sync Cloud -> Local Cache
        db.sync_cloud_to_local()
        
        # Fetch Trips Configuration for dynamic photo sorting
        active_trips = db.get_trips()
        logger.info(f"Loaded {len(active_trips)} trip configurations from Database")
        
    except Exception as e:
        logger.error(f"Database Init Failed: {e}")
        return

    # 2. Account Setup
    email, remote, acc_idx = get_active_account_info()
    creds = get_creds(email)
    if not creds:
        logger.error(f"❌ Auth failed for {email}. Check tokens.")
        return

    session_uploads = [] # List of {"filename":, "size":, "account":}
    session_total_size = 0

    # 3. Scanning directories
    should_restart = False
    
    for folder in SOURCE_DIRECTORIES:
        if should_restart: break
        if not os.path.exists(folder): continue
            
        logger.info(f"📂 Scanning: {folder}")
        for root, _, files in os.walk(folder):
            if should_restart: break
            
            for file in files:
                if not file.lower().endswith(VALID_EXTENSIONS): continue
                
                filepath = os.path.join(root, file)
                filesize = os.path.getsize(filepath)
                
                # --- PHASE 1: Filename Check (Fast) ---
                if db.file_exists_by_name(file):
                    continue # SKIP, already exists by name
                
                # --- PHASE 2: Check Storage before doing work ---
                # Check every, say, 10 uploads or just check on error?
                # To be safe and since rclone is slow, maybe check ONLY if we suspect full?
                # The user asked to check if > 90%. Let's check periodically or before big uploads?
                # For simplicity, let's assume we check each time or rely on an assumption.
                # To avoid spamming rclone calls, let's check ONLY if we decide to upload.
                
                # --- PHASE 3: Hash Check (Deep) ---
                # Only if filename was unknown
                f_hash = calculate_file_hash(filepath)
                if db.file_exists_by_hash(f_hash):
                    # It's a renamed duplicate. Skip.
                    # Optional: We could insert the alias into DB here to speed up next time?
                    # db.insert_file(....) # skipped for now to save DB calls
                    continue
                
                # If we get here, it's a NEW file. Check storage now.
                usage = get_storage_usage(remote)
                if usage >= 90:
                    if switch_account(acc_idx, email, usage):
                        should_restart = True
                        break
                    else:
                        logger.error("Stopping due to full storage and no backup accounts.")
                        should_restart = True # Actually stop
                        break

                # --- PHASE 4: Album Sorting ---
                trip_info = get_assigned_album(filepath, active_trips)
                album_id = None
                album_name = None
                
                if trip_info:
                     album_name = trip_info.get("name")
                     saved_album_id = trip_info.get("album_id")
                     logger.info(f"🎯 Sorting into Album: {album_name}")
                     album_id, new_saved_id = get_or_create_album(creds, album_name, db, email, saved_album_id)
                     
                     # Update active_trips dynamically so subsequent photos in this run use the updated JSON
                     if new_saved_id and new_saved_id != saved_album_id:
                         for t in active_trips:
                             if t["name"] == album_name:
                                 t["album_id"] = new_saved_id
                                 break

                # --- PHASE 5: Upload ---
                logger.info(f"📤 Uploading: {file} ({filesize/1024/1024:.2f} MB)")
                success, _ = upload_file_to_google(creds, filepath, album_id)
                
                if success:
                    logger.info(f"✅ Success: {file}")
                    
                    # Log to DB
                    db.insert_file({
                        "file_hash": f_hash,
                        "filename": file,
                        "file_size_bytes": filesize,
                        "account_email": email,
                        "device_source": DEVICE_NAME,
                        "remote_id": "google_photos_api", # We could extract true ID from response
                        "album_name": album_name # Store album name
                    })
                    
                    # Track Stats
                    session_uploads.append({
                        "filename": file,
                        "size": filesize,
                        "account": email
                    })
                    session_total_size += filesize
                else:
                    logger.error(f"❌ Upload Failed: {file}")

    if should_restart:
        logger.info("🔄 Restarting session with new account...")
        return main()

    # 4. Final Reporting & Backup
    end_time = datetime.now()
    duration = end_time - start_time
    
    # Always Backup DB
    db.backup_to_local_sqlite(BACKUP_DB_PATH)
    
    if session_uploads:
        total_mb = session_total_size / (1024 * 1024)
        count = len(session_uploads)
        
        # Build Report
        report_lines = [
            f"Subject: Photo Uploader Report - {DEVICE_NAME} - {datetime.now().strftime('%Y-%m-%d')}",
            f"Device: {DEVICE_NAME}",
            f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"Duration: {duration}",
            f"Total Uploads: {count}",
            f"Total Size: {total_mb:.2f} MB",
            "",
            "Files Uploaded:"
        ]
        
        for item in session_uploads:
            report_lines.append(f"- {item['filename']} ({item['size']/1024/1024:.2f} MB) [{item['account']}]")
            
        report_text = "\n".join(report_lines)
        
        # Save Local History
        hist_file = os.path.join(HISTORY_DIR, f"{end_time.strftime('%Y%m%d_%H%M%S')}_report.txt")
        with open(hist_file, "w", encoding="utf-8") as f:
            f.write(report_text)
            
        # Send Email
        # Extract subject from first line
        email_subject = report_lines[0].replace("Subject: ", "")
        email_body = report_text.replace(report_lines[0], "").strip() # Remove subject line from body
        
        send_email(email_subject, email_body)
        logger.info(f"✅ Session Complete. Uploaded {count} files.")
    else:
        logger.info("✅ Session Complete. No new files found.")

if __name__ == "__main__":
    import sys
    LOCKFILE_PATH = os.path.join(BASE_DIR, "uploader_sql.lock")
    
    try:
        fd = os.open(LOCKFILE_PATH, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        with os.fdopen(fd, 'w') as f:
            f.write(str(os.getpid()))
    except FileExistsError:
        logger.error(f"❌ Lock file found at {LOCKFILE_PATH}! Another instance is currently running.")
        sys.exit(1)
    except Exception as e:
        logger.error(f"❌ Could not create lock file: {e}")
        sys.exit(1)

    logger_process = None
    try:
        logger_process = subprocess.Popen([sys.executable, os.path.join(BASE_DIR, "logger.py")])
        main()
    finally:
        if logger_process:
            try:
                logger_process.terminate()
                logger_process.wait(timeout=5)
            except Exception as e:
                logger.error(f"❌ Failed to stop logger process: {e}")
                
        if os.path.exists(LOCKFILE_PATH):
            try:
                os.remove(LOCKFILE_PATH)
            except Exception as e:
                logger.error(f"❌ Failed to delete lock file: {e}")
