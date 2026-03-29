#!/usr/bin/env python3
import os
import sys
import queue
import threading
import shutil
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# Infra
import infra.logger as logger
from infra.auth import get_active_account_info, get_creds, get_storage_usage, send_email, ACCOUNTS

# DB
from db.balancer import DatabaseManager

# Core Workers
from core.scanner import scanner_worker, load_ignore_set
from core.deduplicator import deduplicator_worker
from core.uploader import upload_one
from core.tracker import track_one
from core.init_wizard import run_init_wizard
from core.thumbnail_generator import thumbnail_worker

# Config
DEVICE_NAME = os.getenv("DEVICE_NAME", "Unknown_Device")
BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def _validate_env_for_pipeline() -> bool:
    """Check required env vars for the upload pipeline. Returns False if validation fails."""
    missing = []
    if not os.getenv("NHOST_DB_URL") and not os.getenv("NEON_DB_URL"):
        missing.append("at least one of NHOST_DB_URL or NEON_DB_URL")
    if not os.getenv("GOOGLE_ACCOUNTS", "").strip():
        missing.append("GOOGLE_ACCOUNTS (comma-separated Google account emails)")
    if missing:
        logger.error("❌ Missing required environment variables for the upload pipeline:")
        for m in missing:
            logger.error(f"   - {m}")
        logger.error("   Copy .env.example to .env and fill in the values, or run: python main.py init")
        return False
    return True


def _check_system_dependencies() -> bool:
    """Check for required system packages. Returns False if any are missing."""
    required_tools = ["ffmpeg"]
    missing = []
    for tool in required_tools:
        if shutil.which(tool) is None:
            missing.append(tool)

    if missing:
        logger.error("❌ Missing required system dependencies:")
        for m in missing:
            logger.error(f"   - {m}")
        if "ffmpeg" in missing:
            logger.error("   Please install ffmpeg to enable video thumbnail generation.")
        return False
    return True
DATA_DIR = os.path.join(BASE_DIR, "Data")
HISTORY_DIR = os.path.join(BASE_DIR, "UploadHistory")
FILENAME_CACHE_FILE = os.path.join(DATA_DIR, "filename_cache.txt")
BACKUP_DB_PATH = os.path.join(DATA_DIR, "Backups", f"backup_{DEVICE_NAME}.db")

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(os.path.dirname(BACKUP_DB_PATH), exist_ok=True)
os.makedirs(HISTORY_DIR, exist_ok=True)




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


def main(dry_run=False, _restart_count=0):
    if dry_run:
        logger.info("🏜️ Starting Photo Uploader (Pipeline Edition) in DRY RUN mode...")
    else:
        logger.info("🚀 Starting Photo Uploader (Pipeline Edition)...")
    start_time = datetime.now()

    # Runtime cache for Albums { "Album Name": "album_id" } — local to this run
    ALBUMS_CACHE = {}

    # Load ignore list from Data/.ignore
    ignore_set = load_ignore_set()

    local_filename_cache = load_filename_cache()
    
    # 1. Init Database
    try:
        db = DatabaseManager(use_local_cache=True)
        if not db.check_connection():
            logger.error("Failed to connect to Nhost. Exiting.")
            return
        
        # Sync Cloud -> Local Cache
        db.sync_cloud_to_local()
        
        # Prime filename cache from DB so we don't re-upload if cache file was out of sync (e.g. crash after DB insert)
        db_filenames = db.get_all_media_filenames()
        if db_filenames:
            before = len(local_filename_cache)
            local_filename_cache |= db_filenames
            added = len(local_filename_cache) - before
            if added:
                logger.info(f"Primed filename cache with {added} entries from database (total {len(local_filename_cache)}).")
        
        active_trips = db.get_trips()
        logger.info(f"Loaded {len(active_trips)} trip configurations from Database")

        source_directories = db.get_device_directories(DEVICE_NAME)
        if not source_directories:
            logger.warning(f"⚠️ No directories configured for device '{DEVICE_NAME}'. Please add them via query_db.py.")
            return
        else:
            logger.info(f"Loaded {len(source_directories)} source directories from Database for device '{DEVICE_NAME}'.")
    except Exception as e:
        logger.error(f"Database Init Failed: {e}")
        return

    # 2. Account Setup
    email, remote, acc_idx = get_active_account_info()
    creds = get_creds(email)
    if not creds:
        logger.error(f"❌ Auth failed for {email}. Check tokens.")
        return

    # 3. Pipeline Setup
    scanner_out = queue.Queue(maxsize=100)
    thumbnail_out = queue.Queue()

    # Shared mutable state
    shared_state = {
        "lock": threading.Lock(),
        "acc_idx": acc_idx,
        "email": email,
        "remote": remote,
        "creds": creds,
        "should_restart": False,
        "session_uploads": [],
        "session_total_size": 0
    }

    # Worker Contexts
    upload_ctx = {
        "db": db,
        "active_trips": active_trips,
        "device_name": DEVICE_NAME,
        "albums_cache": ALBUMS_CACHE,
        "accounts": ACCOUNTS,
        "shared_state": shared_state
    }
    
    tracker_ctx = {
        "db": db,
        "device_name": DEVICE_NAME,
        "local_filename_cache": local_filename_cache,
        "append_to_filename_cache": append_to_filename_cache,
        "shared_state": shared_state,
        "thumbnail_queue": thumbnail_out
    }

    # Thumbnailer runs as a background thread throughout both phases
    thumbnail_thread = threading.Thread(target=thumbnail_worker, args=(thumbnail_out,), daemon=True)
    thumbnail_thread.start()

    # -------------------------------------------------------------------------
    # PHASE 1: Scan + Deduplicate (concurrent) — build a list of files to upload
    # -------------------------------------------------------------------------
    logger.info("="*50)
    logger.info("🔍 Phase 1: Scanning and deduplicating...")
    files_to_upload = []  # deduplicator appends here instead of a live queue

    scanner_thread = threading.Thread(
        target=scanner_worker,
        args=(source_directories, scanner_out, ignore_set)
    )
    dedup_thread = threading.Thread(
        target=deduplicator_worker,
        args=(scanner_out, files_to_upload, db, local_filename_cache, append_to_filename_cache, dry_run)
    )

    scanner_thread.start()
    dedup_thread.start()
    scanner_thread.join()
    dedup_thread.join()

    logger.info(f"🔍 Phase 1 complete. {len(files_to_upload)} new file(s) queued for upload.")
    logger.info("="*50)

    # -------------------------------------------------------------------------
    # PHASE 2: Upload + Track (strictly sequential) — one file at a time
    # -------------------------------------------------------------------------
    if files_to_upload:
        logger.info("📤 Phase 2: Starting sequential upload...")
        for item in files_to_upload:
            # Check if a previous iteration triggered a storage restart
            if shared_state.get("should_restart"):
                logger.info("🔄 Storage limit reached mid-session, stopping Phase 2 early.")
                logger.info("="*50)                
                break

            result = upload_one(item, upload_ctx, dry_run)

            if result is None:
                # Upload failed — already logged inside upload_one
                continue

            if isinstance(result, dict) and result.get("type") in ("restart", "stop"):
                logger.info(f"⚠️  Pipeline control signal: {result['type']}. Stopping Phase 2.")
                logger.info("="*50)
                break

            track_one(result, tracker_ctx, dry_run)

    # Signal thumbnailer to finish and wait for it
    thumbnail_out.put(None)
    thumbnail_thread.join()

    # Check if a restart was scheduled (e.g. account out of space)
    if shared_state["should_restart"]:
        max_restarts = len(ACCOUNTS)
        if _restart_count >= max_restarts:
            logger.error(f"❌ Max restart count ({max_restarts}) reached. Stopping to prevent infinite loop.")
            return
        logger.info(f"🔄 Restarting session with new account (attempt {_restart_count + 1}/{max_restarts})...")
        ALBUMS_CACHE.clear()
        return main(dry_run=dry_run, _restart_count=_restart_count + 1)

    # 4. Final Reporting & Backup
    end_time = datetime.now()
    duration = end_time - start_time
    
    if not dry_run:
        db.backup_to_local_sqlite(BACKUP_DB_PATH)
    
    session_uploads = shared_state["session_uploads"]
    session_total_size = shared_state["session_total_size"]

    if session_uploads:
        total_mb = session_total_size / (1024 * 1024)
        count = len(session_uploads)
        
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
        
        hist_file = os.path.join(HISTORY_DIR, f"{end_time.strftime('%Y%m%d_%H%M%S')}_report.txt")
        with open(hist_file, "w", encoding="utf-8") as f:
            f.write(report_text)
            
        email_subject = report_lines[0].replace("Subject: ", "")
        email_body = report_text.replace(report_lines[0], "").strip()
        
        send_email(email_subject, email_body, device_name=DEVICE_NAME)
        
        if dry_run:
            logger.info(f"🏜️ [DRY RUN] Report emailed.")
            logger.info(f"✅ Session Complete. Would have uploaded {count} files ({total_mb:.2f} MB).")
        else:
            logger.info(f"✅ Session Complete. Uploaded {count} files ({total_mb:.2f} MB).")
    else:
        logger.info("✅ Session Complete. No new files found.")

def _is_pid_alive(pid: int) -> bool:
    """Return True if a process with the given PID is still running (cross-platform)."""
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def _acquire_lock(lockfile_path: str) -> bool:
    """Create lock file; if it exists, remove it when the recorded PID is no longer running. Returns True if lock acquired."""
    try:
        fd = os.open(lockfile_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        with os.fdopen(fd, 'w') as f:
            f.write(str(os.getpid()))
        return True
    except FileExistsError:
        try:
            with open(lockfile_path, 'r') as f:
                raw = f.read().strip()
                pid = int(raw) if raw else 0
        except (ValueError, OSError):
            pid = 0
        if pid and _is_pid_alive(pid):
            logger.error(f"❌ Lock file at {lockfile_path} held by running process (PID {pid}). Exiting.")
            return False
        logger.warning(f"⚠️ Removing stale lock file (PID {pid} no longer running).")
        try:
            os.remove(lockfile_path)
        except OSError as e:
            logger.error(f"❌ Could not remove stale lock file: {e}")
            return False
        return _acquire_lock(lockfile_path)
    except Exception as e:
        logger.error(f"❌ Could not create lock file: {e}")
        return False


if __name__ == "__main__":
    LOCKFILE_PATH = os.path.join(BASE_DIR, "uploader_pipeline.lock")
    
    if not _acquire_lock(LOCKFILE_PATH):
        sys.exit(1)

    try:
        dry_run_mode = False
        
        if len(sys.argv) > 1 and sys.argv[1] == "init":
            run_init_wizard()
            sys.exit(0)

        if not _validate_env_for_pipeline():
            sys.exit(1)
            
        if not _check_system_dependencies():
            sys.exit(1)
            
        if ("--dry-run" in sys.argv) or (os.getenv("DRY_RUN") == "True"):
            dry_run_mode = True

        main(dry_run=dry_run_mode)
    finally:
        if os.path.exists(LOCKFILE_PATH):
            try:
                os.remove(LOCKFILE_PATH)
            except Exception as e:
                logger.error(f"❌ Failed to delete lock file: {e}")
