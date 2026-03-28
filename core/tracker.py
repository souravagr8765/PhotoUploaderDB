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
    thumbnail_queue = context.get("thumbnail_queue")
    state_lock = shared_state.get("lock")

    file = item["filename"]
    filepath = item["filepath"]
    filesize = item["filesize"]
    f_hash = item["hash"]
    status = item["status"]
    album_name = item.get("album_name")
    remote_id = item.get("remote_id")
    email = shared_state["email"]

    # Generate Thumbnail ID — let background worker handle the actual generation
    thumbid = str(uuid.uuid4())
    thumb_success = False
    if not dry_run and status == "success":
        thumb_success = True
        if thumbnail_queue:
            thumbnail_queue.put({
                "filepath": filepath,
                "thumbid": thumbid,
                "filename": file
            })

    # Extract metadata
    date_taken, has_gps = get_photo_metadata(filepath)
    date_taken = extract_date_from_file_fallback(filepath, date_taken)
    upload_date_str = date_taken.isoformat() if date_taken else datetime.now().isoformat()

    # Determine device source — WhatsApp heuristic
    file_device_source = device_name
    if "WA" in file.upper() and not has_gps:
        file_device_source = "Whatsapp"

    # Database Logging
    if not dry_run and status == "success":
        try:
            db.insert_file({
                "file_hash": f_hash,
                "filename": file,
                "file_size_bytes": filesize,
                "upload_date": upload_date_str,
                "account_email": email,
                "device_source": file_device_source,
                "remote_id": remote_id,
                "album_name": album_name,
                "thumbid": thumbid if thumb_success else None
            })
        except Exception as e:
            logger.error(f"❌ DB Insert Failed for {file}: {e}")

    # Update in-memory cache
    if not dry_run: 
        local_filename_cache.add(file.lower())
        append_to_filename_cache(file)

    # Track stats — protected by lock for thread safety (thumbnailer runs concurrently)
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
