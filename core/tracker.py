import os
import uuid
from datetime import datetime
import infra.logger as logger
from metadata.extractor import get_photo_metadata, extract_date_from_file_fallback


def track_one(item: dict, context: dict, dry_run: bool = False):
    """
    Processes a single successfully uploaded item through the tracking stage.
    Handles DB insertions, stats tallying, and thumbnail queue push.
    Called sequentially from main.py Phase 2 loop.
    """
    db = context["db"]
    device_name = context["device_name"]
    local_filename_cache = context["local_filename_cache"]
    append_to_filename_cache = context["append_to_filename_cache"]
    shared_state = context["shared_state"]
    state_lock = shared_state.get("lock")

    file = item["filename"]
    filepath = item["filepath"]
    filesize = item["filesize"]
    f_hash = item["hash"]
    status = item["status"]
    album_name = item.get("album_name")
    remote_id = item.get("remote_id")
    email = shared_state["email"]

    # Extract metadata (Prefer cached metadata from Phase 1.5)
    cached_meta = item.get("metadata")
    if cached_meta:
        date_taken = cached_meta.get("date_taken")
        has_gps = cached_meta.get("has_gps")
    else:
        # Fallback if metadata wasn't cached (e.g. manual call or bug)
        date_taken, has_gps, *extra = get_photo_metadata(filepath)
        date_taken = extract_date_from_file_fallback(filepath, date_taken)
    
    upload_date_str = date_taken.isoformat() if date_taken else datetime.now().isoformat()

    # Determine device source — WhatsApp heuristic
    file_device_source = device_name
    if "WA" in file.upper() and not has_gps:
        file_device_source = "Whatsapp"

    # Database Logging
    if not dry_run and status == "success":
        try:
            db.insert_file_async({
                "file_hash": f_hash,
                "filename": file,
                "file_size_bytes": filesize,
                "upload_date": upload_date_str,
                "account_email": email,
                "device_source": file_device_source,
                "remote_id": remote_id,
                "album_name": album_name
            })
        except Exception as e:
            logger.error(f"❌ DB Async Insert Queuing Failed for {file}: {e}")

    # Update in-memory cache
    if not dry_run: 
        local_filename_cache.add(file.lower())
        append_to_filename_cache(file)

    # Track stats — protected by lock for thread safety
    if state_lock:
        with state_lock:
            shared_state["session_uploads"].append({
                "filename": file,
                "size": filesize,
                "account": email
            })
            shared_state["session_total_size"] += filesize
    else:
        shared_state["session_uploads"].append({
            "filename": file,
            "size": filesize,
            "account": email
        })
        shared_state["session_total_size"] += filesize
