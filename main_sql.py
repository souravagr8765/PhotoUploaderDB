#!/usr/bin/env python3
import logger
from fileinput import filename
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
import uuid
from tqdm import tqdm
import re

# Import our new Database Manager
from database import DatabaseManager
from metadata_engine import get_assigned_album, get_photo_metadata
from thumbnail_generator import generate_thumbnail
load_dotenv()

# ===== GLOBAL CONFIG =====
DEVICE_NAME = os.getenv("DEVICE_NAME", "Unknown_Device")
ACCOUNTS = ["souravagarwalchildrensday@gmail.com", "ca.aspirant.sourav.agarwal@gmail.com","photouploader.sourav@gmail.com","photouploader.souravagarwal@gmail.com"] 
smtp_server = os.getenv("SMTP_SERVER", "smtp.gmail.com")
smtp_port = os.getenv("SMTP_PORT", "587")
SENDER_EMAIL = os.getenv("SENDER_EMAIL")
RECEIVER_EMAIL = os.getenv("RECEIVER_EMAIL")
APP_PASSWORD = os.getenv("APP_PASSWORD")

# --- MULTI-FOLDER LIST ---
_source_dirs_env = os.getenv("SOURCE_DIRECTORIES", "E:\\FamilyMemories\\library\\admin")
SOURCE_DIRECTORIES = [d.strip() for d in _source_dirs_env.split(",") if d.strip()]

# Path Config
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "Data")
HISTORY_DIR = os.path.join(BASE_DIR, "UploadHistory")
ACTIVE_ACC_FILE = os.path.join(DATA_DIR, "active_account.txt")
BACKUP_DB_PATH = os.path.join(DATA_DIR, "Backups", f"backup_{DEVICE_NAME}.db")
FILENAME_CACHE_FILE = os.path.join(DATA_DIR, "filename_cache.txt")
LOGFILE = os.path.join(BASE_DIR, "uploader_sql.log")

VALID_EXTENSIONS = ('.jpg', '.jpeg', '.png', '.gif', '.bmp',
                    '.mp4', '.mov', '.avi', '.mkv', '.webm')

# Ensure directories exist
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(os.path.dirname(BACKUP_DB_PATH), exist_ok=True)
os.makedirs(HISTORY_DIR, exist_ok=True)

# Runtime Cache for Albums { "Album Name": "album_id" }
ALBUMS_CACHE = {}

# ===== Utilities =====

def load_filename_cache():
    cache = set()
    if os.path.exists(FILENAME_CACHE_FILE):
        with open(FILENAME_CACHE_FILE, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    cache.add(line.strip().lower())
    return cache

def append_to_filename_cache(filename):
    with open(FILENAME_CACHE_FILE, "a", encoding="utf-8") as f:
        f.write(filename.lower() + "\n")

def extract_date_from_file_fallback(filepath, date_taken):
    if date_taken:
        return date_taken
        
    filename = os.path.basename(filepath)
    filename_lower = filename.lower()
    
    # 1. WhatsApp
    if "wa" in filename_lower:
        match = re.search(r'(img|vid)-(\d{8})-wa\d+', filename_lower)
        if match:
            try:
                return datetime.strptime(match.group(2), "%Y%m%d")
            except ValueError:
                pass

    # 2. Screenshot
    if "screenshot" in filename_lower:
        match = re.search(r'screenshot_(\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2})', filename_lower)
        if match:
            try:
                return datetime.strptime(match.group(1), "%Y-%m-%d-%H-%M-%S")
            except ValueError:
                pass

    # 3. Regular Photo or Video
    match = re.search(r'(img|vid)(\d{14})', filename_lower)
    if match:
        try:
            return datetime.strptime(match.group(2), "%Y%m%d%H%M%S")
        except ValueError:
            pass

    return None

def calculate_file_hash(filepath: str) -> str:
    """Calculates SHA-256 hash of a file, optimized for large files."""
    sha256_hash = hashlib.sha256()
    file_size = os.path.getsize(filepath)
    # if file_size > 50 * 1024 * 1024:  # 50 MB
    #     logger.info(f"⏳ Calculating hash for large file: {os.path.basename(filepath)} ({file_size/1024/1024:.2f} MB)...")
        
    with open(filepath, "rb") as f:
        # Read in 1MB chunks to dramatically speed up Python loop overhead for large files
        for byte_block in iter(lambda: f.read(1048576), b""):
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
        server = smtplib.SMTP(smtp_server, int(smtp_port))
        server.starttls()
        server.login(SENDER_EMAIL, APP_PASSWORD)
        server.send_message(msg)
        server.quit()
        logger.info(f"📧 Email sent: {subject}")
    except Exception as e:
        logger.error(f"❌ Failed to send email: {e}")

# ===== Account & Storage Management =====

def get_storage_usage(remote):
    wait_for_internet()
    try:
        result = subprocess.run(['rclone', 'about', f'{remote}:', '--json'], capture_output=True, text=True, shell=False, check=True)
        data = json.loads(result.stdout)
        used = data.get("used", 0) + data.get("other", 0)
        total = data.get("total", 1)
        return (used / total) * 100
    except Exception:
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
                    # Request refresh with the same scopes we now need
                    creds.refresh(Request())
                    with open(token_path, "wb") as f_out: 
                        pickle.dump(creds, f_out)
                except Exception as e:
                    logger.error(f"Failed to refresh token: {e}")
                    return None
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
        resp = requests.post('https://photoslibrary.googleapis.com/v1/albums', headers=headers, json=payload, timeout=30)
        
        if resp.status_code == 200:
            data = resp.json()
            album_id = data.get("id")
            album_url = data.get("productUrl", "https://photos.google.com/albums")
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
                        f"🔗 Link to album: {album_url}\n\n"
                        f"IMPORTANT: Please open the link above for {email} and manually share "
                        f"this album with your main account to merge them together!")
                send_email(subject, body)
            else:
                subject = f"📸 New Trip Album Created: {album_name}"
                body = (f"A brand new album was created for trip '{album_name}' "
                        f"on account: {email}.\n\n"
                        f"🔗 Link to album: {album_url}")
                send_email(subject, body)
            
            return album_id, new_saved_id
        else:
            logger.error(f"Failed to create album {album_name}: {resp.text}")
            return None, None
    except Exception as e:
        logger.error(f"Album API Error: {e}")
        return None, None


def upload_file_to_google(creds, path, album_id=None, email=None):
    wait_for_internet()
    
    if (not getattr(creds, 'valid', True) or getattr(creds, 'expired', False)) and getattr(creds, 'refresh_token', None):
        try:
            logger.info("🔑 Token needs refresh before upload, refreshing...")
            creds.refresh(Request())
            if email:
                token_path = os.path.join(BASE_DIR, "creds", f"token_{email}.pkl")
                with open(token_path, "wb") as f_out: 
                    pickle.dump(creds, f_out)
        except Exception as e:
            logger.error(f"Failed to refresh token before upload: {e}")

    filename = os.path.basename(path)
    headers = {
        'Authorization': f'Bearer {creds.token}', 
        'Content-type': 'application/octet-stream', 
        'X-Goog-Upload-File-Name': filename, 
        'X-Goog-Upload-Protocol': 'raw'
    }
    try:
        file_size = os.path.getsize(path)
        headers['Content-Length'] = str(file_size)
        with open(path, 'rb') as f:
            with tqdm.wrapattr(f, "read", total=file_size, desc=f"Uploading {filename}", unit="B", unit_scale=True, unit_divisor=1024, miniters=1) as wrapped_file:
                resp = requests.post('https://photoslibrary.googleapis.com/v1/uploads', data=wrapped_file, headers=headers, timeout=600)
        
        if resp.status_code == 200:
            upload_token = resp.text
            
            # Create Media Item
            body = {"newMediaItems": [{"simpleMediaItem": {"uploadToken": upload_token}}]}
            
            # Add to Album if specified
            if album_id:
                body["albumId"] = album_id
                
            if (not getattr(creds, 'valid', True) or getattr(creds, 'expired', False)) and getattr(creds, 'refresh_token', None):
                try:
                    logger.info("🔑 Token expired during upload, refreshing for batchCreate...")
                    creds.refresh(Request())
                    if email:
                        token_path = os.path.join(BASE_DIR, "creds", f"token_{email}.pkl")
                        with open(token_path, "wb") as f_out: 
                            pickle.dump(creds, f_out)
                except Exception as e:
                    logger.error(f"Failed to refresh token before batchCreate: {e}")
                
            create_resp = requests.post(
                'https://photoslibrary.googleapis.com/v1/mediaItems:batchCreate',
                headers={'Authorization': f'Bearer {creds.token}', 'Content-type': 'application/json'},
                json=body,
                timeout=60
            )
            # Check if any items were successfully created
            if create_resp.status_code == 200:
                res_json = create_resp.json()
                # Check specifics if needed, but for now allow generic success
                return True, res_json
            else:
                logger.error(f"batchCreate error {create_resp.status_code}: {create_resp.text}")
        else:
            logger.error(f"Upload endpoint error {resp.status_code}: {resp.text}")
    except Exception as e:
        logger.error(f"Upload API Error: {e}")
        return False, None
    return False, None

# ===== Main Loop =====

def main(dry_run=False):
    if dry_run:
        logger.info("🏜️ Starting Photo Uploader (SQL Edition) in DRY RUN mode...")
    else:
        logger.info("🚀 Starting Photo Uploader (SQL Edition)...")
    start_time = datetime.now()
    
    # Load fast local text cache
    local_filename_cache = load_filename_cache()
    
    # 1. Init Database
    try:
        db = DatabaseManager(use_local_cache=True)
        if not db.check_connection():
            logger.error("Failed to connect to Nhost. Exiting.")
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
                if file.lower().startswith('.trashed'): continue
                if file.lower().startswith('vid20251028070057'): continue

                filepath = os.path.join(root, file)
                filesize = os.path.getsize(filepath)

                # --- PHASE 0: In-Memory Fast Cache Check ---
                if file.lower() in local_filename_cache:
                    continue # SKIP entirely without DB or logging to save time

                logger.info(f"Checking for the file in DB: {file}")
                # --- PHASE 1: Filename Check (Fast) ---
                if db.file_exists_by_name(file):
                    logger.info(f"File already exists in DB(By Name): {file}")
                    local_filename_cache.add(file.lower())
                    append_to_filename_cache(file)
                    continue # SKIP, already exists by name
                    
                # --- PHASE 2: Check Storage before doing work ---
                # Check every, say, 10 uploads or just check on error?
                # To be safe and since rclone is slow, maybe check ONLY if we suspect full?
                # The user asked to check if > 90%. Let's check periodically or before big uploads?
                # For simplicity, let's assume we check each time or rely on an assumption.
                # To avoid spamming rclone calls, let's check ONLY if we decide to upload.
                
                # --- PHASE 3: Hash Check (Deep) ---
                # Only if filename was unknown
                logger.info(f"filename not found in the Database,Calculating hash for the file: {file}")
                f_hash = calculate_file_hash(filepath)
                original_file_data = db.get_file_by_hash(f_hash)
                if original_file_data:
                    logger.info(f"File already exists in DB(by HASH): {file}")
                    # It's a renamed duplicate. Skip upload, but log as an alias.
                    new_file_data = dict(original_file_data)
                    new_file_data["filename"] = file
                    
                    # Estimate capture date for the alias record
                    date_taken, _ = get_photo_metadata(filepath)
                    date_taken = extract_date_from_file_fallback(filepath, date_taken)
                    upload_date_str = date_taken.isoformat() if date_taken else datetime.now().isoformat()
                    new_file_data["upload_date"] = upload_date_str
                    
                    if not dry_run:
                        db.insert_file(new_file_data)
                        logger.info(f"Added alias to DB: {file}")
                    else:
                        logger.info(f"🏜️ [DRY RUN] Would add alias to DB: {file}")
                        
                    local_filename_cache.add(file.lower())
                    append_to_filename_cache(file)
                    continue
                
                # If we get here, it's a NEW file. Check storage now.
                if not dry_run:
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
                     
                if dry_run:
                    logger.info(f"🏜️ [DRY RUN] Would upload: {file} ({filesize/1024/1024:.2f} MB) -> Album: {album_name}")
                    session_uploads.append({
                        "filename": file,
                        "size": filesize,
                        "account": email
                    })
                    session_total_size += filesize
                    continue
                     
                if trip_info:
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
                success, _ = upload_file_to_google(creds, filepath, album_id, email=email)
                
                if success:
                    logger.info(f"✅ Success: {file}")
                    
                    # Generate a unique thumbnail ID
                    thumbid = str(uuid.uuid4())
                    
                    # Generate the physical thumbnail
                    thumb_success = generate_thumbnail(filepath, thumbid)
                    if not thumb_success:
                        logger.warning(f"⚠️ Thumbnail generation failed for {file}, but upload succeeded.")
                    
                    # Determine device source based on filename and EXIF
                    file_device_source = DEVICE_NAME
                    if "WA" in file.upper():
                        has_exif_device = False
                        if file.lower().endswith(('.jpg', '.jpeg', '.heic', '.png', '.webp', '.bmp', '.gif')):
                            try:
                                from PIL import Image
                                with Image.open(filepath) as img:
                                    exif = img.getexif() if hasattr(img, 'getexif') else getattr(img, '_getexif', lambda: None)()
                                    if exif and (271 in exif or 272 in exif):
                                        has_exif_device = True
                            except Exception:
                                pass
                        if not has_exif_device:
                            file_device_source = "Whatsapp"

                    # Log to DB
                    date_taken, _ = get_photo_metadata(filepath)
                    date_taken = extract_date_from_file_fallback(filepath, date_taken)
                    upload_date_str = date_taken.isoformat() if date_taken else datetime.now().isoformat()
                    
                    db.insert_file({
                        "file_hash": f_hash,
                        "filename": file,
                        "file_size_bytes": filesize,
                        "upload_date": upload_date_str,
                        "account_email": email,
                        "device_source": file_device_source,
                        "remote_id": "google_photos_api", # We could extract true ID from response
                        "album_name": album_name, # Store album name
                        "thumbid": thumbid if thumb_success else None # Store the generated ID
                    })
                    
                    # Track Stats
                    local_filename_cache.add(file.lower())
                    append_to_filename_cache(file)
                    
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
        return main(dry_run=dry_run)

    # 4. Final Reporting & Backup
    end_time = datetime.now()
    duration = end_time - start_time
    
    # Always Backup DB
    if not dry_run:
        db.backup_to_local_sqlite(BACKUP_DB_PATH)
    
    if session_uploads:
        total_mb = session_total_size / (1024 * 1024)
        count = len(session_uploads)
        
        # Build Report
        report_lines = [
            f"Subject: {'[DRY RUN] ' if dry_run else ''}Photo Uploader Report - {DEVICE_NAME} - {datetime.now().strftime('%Y-%m-%d')}",
            f"Device: {DEVICE_NAME}",
            f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"Mode: {'DRY RUN (No files were uploaded)' if dry_run else 'LIVE'}",
            f"Duration: {duration}",
            f"{'Total Files to be Uploaded' if dry_run else 'Total Uploads'}: {count}",
            f"Total Size: {total_mb:.2f} MB",
            "",
            "Files to be Uploaded:" if dry_run else "Files Uploaded:"
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
        
        # Send email regardless of dry_run, since user wants the report
        send_email(email_subject, email_body)
        
        if dry_run:
            logger.info(f"🏜️ [DRY RUN] Report emailed.")
            logger.info(f"✅ Session Complete. Would have uploaded {count} files ({total_mb:.2f} MB).")
        else:
            logger.info(f"✅ Session Complete. Uploaded {count} files ({total_mb:.2f} MB).")
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

    try:
        dry_run_mode = False
        if ("--dry-run" in sys.argv) | (os.getenv("DRY_RUN") == "True"):
            dry_run_mode = True

        main(dry_run=dry_run_mode)
    finally:
        if os.path.exists(LOCKFILE_PATH):
            try:
                os.remove(LOCKFILE_PATH)
            except Exception as e:
                logger.error(f"❌ Failed to delete lock file: {e}")
