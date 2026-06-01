import os
import requests
import pickle
from tqdm import tqdm
from google.auth.transport.requests import Request
import infra.logger as logger
from infra.auth import wait_for_internet, get_storage_usage, switch_account, get_account_info, get_creds, get_active_account_info
from infra.config_loader import get_config
from metadata.album_router import get_assigned_album, get_or_create_album

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

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
            if create_resp.status_code == 200:
                res_json = create_resp.json()
                # Extract the real Google Photos media item ID
                media_id = None
                results = res_json.get("newMediaItemResults", [])
                if results:
                    media_id = results[0].get("mediaItem", {}).get("id")
                return True, media_id
            else:
                logger.error(f"batchCreate error {create_resp.status_code}: {create_resp.text}")
        else:
            logger.error(f"Upload endpoint error {resp.status_code}: {resp.text}")
    except Exception as e:
        logger.error(f"Upload API Error: {e}")
        return False, None
    return False, None


def upload_one(item: dict, context: dict, dry_run: bool = False, index: int = 0, total: int = 0) -> dict | None:
    """
    Processes a single file item through the upload stage.
    Returns the enriched item dict on success (for tracker), or None on failure/skip.
    Called sequentially from main.py Phase 2 loop.
    """
    db = context["db"]
    active_trips = context["active_trips"]
    device_name = context["device_name"]
    shared_state = context["shared_state"]
    albums_cache = context["albums_cache"]
    accounts = context["accounts"]

    # Stop if a previous iteration triggered a restart (e.g. full storage)
    if shared_state.get("should_restart"):
        return {"type": "restart", "item": item}

    file = item["filename"]
    filepath = item["filepath"]
    filesize = item["filesize"]

    # Initialize caches if not present
    if "storage_cache" not in shared_state:
        shared_state["storage_cache"] = {} # email -> (used, total)
    if "creds_cache" not in shared_state:
        shared_state["creds_cache"] = {} # email -> creds

    # Progress string for logging
    progress_str = f"[{index}/{total}]" if total > 0 else ""

    target_email = None
    target_creds = None
    target_acc_idx = -1

    if not dry_run:
        start_idx = shared_state["acc_idx"]
        for i in range(start_idx, len(accounts)):
            email, remote = get_account_info(i)
            
            # Ensure we have creds
            if email not in shared_state["creds_cache"]:
                shared_state["creds_cache"][email] = get_creds(email)
            c = shared_state["creds_cache"][email]
            if not c:
                logger.error(f"{progress_str} ❌ Could not load credentials for {email}. Skipping account.")
                continue

            # Ensure we have storage info
            if email not in shared_state["storage_cache"]:
                used, tot = get_storage_usage(remote, creds=c)
                shared_state["storage_cache"][email] = (used, tot)
            used, tot = shared_state["storage_cache"][email]
            
            usage_percent = (used / tot) * 100
            
            # Permanent switch if the current active account is already near full (>89%)
            # This ensures we move to the next account as the default before hitting the 90% hard limit.
            if i == shared_state["acc_idx"] and usage_percent >= 89:
                logger.warning(f"{progress_str} ⚠️ Current account {email} is nearly full ({usage_percent:.2f}%). Permanently switching default.")
                if switch_account(i, email, usage_percent, albums_cache, device_name):
                    # Update shared_state with new active account info
                    new_email, new_remote, new_idx = get_active_account_info()
                    shared_state["acc_idx"] = new_idx
                    shared_state["email"] = new_email
                    shared_state["remote"] = new_remote
                    shared_state["creds"] = get_creds(new_email)
                    continue
                else:
                    shared_state["should_restart"] = True
                    return {"type": "stop"}

            # Check if file fits in this account
            projected_usage = ((used + filesize) / tot) * 100
            if projected_usage < 90:
                target_email = email
                target_creds = c
                target_acc_idx = i
                break
            else:
                logger.info(f"{progress_str} ℹ️ Account {email} cannot fit {file} (would reach {projected_usage:.2f}%). Trying next account...")
                continue

        if not target_email:
            logger.error(f"{progress_str} ❌ No available account can accommodate {file} ({filesize/1024/1024:.2f} MB) under the 90% limit.")
            shared_state["should_restart"] = True
            return {"type": "stop"}

        email = target_email
        creds = target_creds
    else:
        # For dry run, just use the current active account
        email = shared_state["email"]
        creds = shared_state["creds"]

    # Try to use pre-calculated album from Phase 1.5 (AI filtering)
    album_name = item.get("album_name")
    album_mgmt_enabled = get_config("app.album_management", True)
    trip_info = None

    if album_mgmt_enabled:
        if album_name:
            # Find trip_info in active_trips to get album_id/url etc.
            trip_info = next((t for t in active_trips if t["name"] == album_name), None)
        else:
            # Fallback to dynamic lookup (should be avoided if AI filtering is enabled)
            trip_info, _, is_excluded, metadata = get_assigned_album(filepath, active_trips)
            if is_excluded:
                logger.info(f"{progress_str} 🚫 Skipping upload (Hard Exclude): {file}")
                return None
            if trip_info:
                album_name = trip_info.get("name")
    else:
        # If album management is disabled, ensure album_name is None
        album_name = None

    if dry_run:
        logger.info(f"{progress_str} 🏜️ [DRY RUN] Would upload: {file} ({filesize/1024/1024:.2f} MB) -> Album: {album_name if album_name else 'Main Library'}")
        item["status"] = "dry_run"
        item["album_name"] = album_name
        return item

    album_id = None
    if album_mgmt_enabled and trip_info:
        saved_album_id = trip_info.get("album_id")
        saved_album_url = trip_info.get("album_url")
        logger.info(f"{progress_str} 🎯 Sorting into Album: {album_name} [{email}]")
        
        # Use account-specific album cache
        if email not in albums_cache:
            albums_cache[email] = {}
        acc_album_cache = albums_cache[email]
        
        album_id, new_saved_id = get_or_create_album(creds, album_name, db, email, accounts, acc_album_cache, saved_album_id, saved_album_url)

        if new_saved_id and new_saved_id != saved_album_id:
            for t in active_trips:
                if t["name"] == album_name:
                    t["album_id"] = new_saved_id
                    break

    logger.info(f"{progress_str} 📤 Uploading: {file} ({filesize/1024/1024:.2f} MB) to {email}")
    success, media_id = upload_file_to_google(creds, filepath, album_id, email=email)

    if success:
        logger.info(f"{progress_str} ✅ Success: {file}")
        item["status"] = "success"
        item["album_name"] = album_name
        item["remote_id"] = media_id
        item["account"] = email
        
        # Update storage cache locally
        used, tot = shared_state["storage_cache"][email]
        shared_state["storage_cache"][email] = (used + filesize, tot)
        
        return item
    else:
        logger.error(f"{progress_str} ❌ Upload Failed: {file}")
        return None
