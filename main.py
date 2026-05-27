#!/usr/bin/env python3
import os
import sys
import queue
import threading
import shutil
from datetime import datetime
from infra.config_loader import get_config

# Reporter
from reporter.state_updater import updater

# Infra
import infra.logger as logger
from infra.auth import get_active_account_info, get_creds, get_storage_usage, ACCOUNTS

# DB
from db.balancer import DatabaseManager

# Core Workers
from core.scanner import scanner_worker, load_ignore_set
from core.deduplicator import deduplicator_worker
from core.uploader import upload_one
from core.tracker import track_one
from core.init_wizard import run_init_wizard
from core.album_sync import sync_all_trips

# Gmail IMAP
from infra.gmail_imap import check_for_album_url

# Config
DEVICE_NAME = get_config("app.device_name", "Unknown_Device")
BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def _validate_env_for_pipeline() -> bool:
    """Check required configuration for the upload pipeline. Returns False if validation fails."""
    missing = []
    nhost_active = get_config("database.nhost_enabled", True) and get_config("database.nhost_url")
    neon_active = get_config("database.neon_enabled", True) and get_config("database.neon_url")
    
    if not nhost_active and not neon_active:
        missing.append("at least one enabled database with a valid URL in config.yaml")
    if not ACCOUNTS:
        missing.append("app.google_accounts list in config.yaml")
    if missing:
        logger.error("❌ Missing required configuration for the upload pipeline:")
        for m in missing:
            logger.error(f"   - {m}")
        logger.error("   Update config.yaml with the required values, or run: python main.py init")
        return False
    return True


def _check_system_dependencies() -> bool:
    """Check for required system packages. Returns False if any are missing."""
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
    updater.update(status="Initializing Database", device=DEVICE_NAME)
    try:
        db = DatabaseManager(use_local_cache=True)
        if not db.check_connection():
            logger.error("Failed to connect to Nhost. Exiting.")
            return
        
        # Sync Cloud -> Local Cache
        db.sync_cloud_to_local()
        
        # Recalculate storage summary from local database every time
        db.refresh_storage_summary(use_local_for_calc=True)
        
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

    # Sync Google Photos Album Removals
    if not dry_run:
        sync_all_trips(db, creds, active_trips, email)
    else:
        logger.info("🏜️ Skipping Album Sync in DRY RUN mode.")

    # -------------------------------------------------------------------------
    # GMAIL ALBUM URL LOOKUP: Check if any trips have album_id but missing album_url
    # If so, search Gmail replies for the shared album link
    # -------------------------------------------------------------------------
    if not dry_run:
        trips_missing_url = [
            t for t in active_trips
            if t.get("album_id") and not t.get("album_url")
        ]
        if trips_missing_url:
            logger.info(f"📧 Found {len(trips_missing_url)} trip(s) missing album URL. Checking Gmail replies...")
            for trip in trips_missing_url:
                trip_name = trip["name"]
                msg_id = trip.get("email_message_id")

                if msg_id:
                    logger.info(f"📧 Checking Gmail for reply to '{trip_name}' (stored Message-ID)...")
                    found_url, _ = check_for_album_url(trip_name, message_id=msg_id)
                else:
                    logger.info(f"📧 No stored Message-ID for '{trip_name}'. Searching by subject...")
                    found_url, discovered_msg_id = check_for_album_url(trip_name, message_id=None)
                    # Save the discovered Message-ID so future runs use the fast path
                    if discovered_msg_id:
                        db.update_trip_message_id(trip_name, discovered_msg_id)
                        logger.info(f"📧 Saved discovered Message-ID for '{trip_name}'")

                if found_url:
                    db.update_trip_album_id(trip_name, trip["album_id"], found_url)
                    logger.info(f"✅ Updated '{trip_name}' with shared album URL: {found_url}")
                    # Also update the in-memory active_trips list for the rest of this run
                    trip["album_url"] = found_url
                else:
                    logger.info(f"📭 No shared album URL found yet for '{trip_name}'. Will retry on next run.")
        else:
            logger.info("✅ All trips have album URLs. Skipping Gmail check.")
    else:
        logger.info("🏜️ Skipping Gmail album URL lookup in DRY RUN mode.")

    # 3. Pipeline Setup
    scanner_out = queue.Queue(maxsize=100)

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
        "shared_state": shared_state
    }

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
    updater.update(files_queued=len(files_to_upload), status="Scanning Complete")

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
            
            # Update Reporter State
            session_uploads = shared_state["session_uploads"]
            session_total_size = shared_state["session_total_size"]
            updater.update(
                total_uploaded=len(session_uploads),
                total_size_mb=round(session_total_size / (1024*1024), 2),
                last_file=item['filename'],
                progress=f"{len(session_uploads)}/{len(files_to_upload)}"
            )

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
    if not dry_run:
        db.backup_to_local_sqlite(BACKUP_DB_PATH)
    
    session_uploads = shared_state["session_uploads"]
    session_total_size = shared_state["session_total_size"]

    if session_uploads:
        total_mb = session_total_size / (1024 * 1024)
        count = len(session_uploads)
        
        # Save a local text history of the upload session
        report_lines = [
            f"Photo Uploader Report - {DEVICE_NAME} - {datetime.now().strftime('%Y-%m-%d')}",
            f"Device: {DEVICE_NAME}",
            f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"Mode: {'DRY RUN' if dry_run else 'LIVE'}",
            f"Duration: {datetime.now() - start_time}",
            f"Total Uploads: {count}",
            f"Total Size: {total_mb:.2f} MB",
            "",
            "Files Uploaded:"
        ]
        for item in session_uploads:
            report_lines.append(f"- {item['filename']} ({item['size']/1024/1024:.2f} MB) [{item['account']}]")
            
        hist_file = os.path.join(HISTORY_DIR, f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_report.txt")
        with open(hist_file, "w", encoding="utf-8") as f:
            f.write("\n".join(report_lines))
        
        updater.update(
            status="Complete", 
            total_uploaded=count, 
            total_size_mb=round(total_mb, 2),
            duration=str(datetime.now() - start_time).split('.')[0]
        )
        
        logger.info(f"✅ Session Complete. Uploaded {count} files ({total_mb:.2f} MB).")
    else:
        updater.update(status="Complete", info="No new files found.")
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
            
        if ("--dry-run" in sys.argv) or (get_config("app.dry_run") == True):
            dry_run_mode = True

        main(dry_run=dry_run_mode)
    finally:
        if os.path.exists(LOCKFILE_PATH):
            try:
                os.remove(LOCKFILE_PATH)
            except Exception as e:
                logger.error(f"❌ Failed to delete lock file: {e}")
