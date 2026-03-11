import os
import requests
import pickle
from tqdm import tqdm
from google.auth.transport.requests import Request
import infra.logger as logger
from infra.auth import wait_for_internet, get_storage_usage, switch_account
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


def upload_one(item: dict, context: dict, dry_run: bool = False) -> dict | None:
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
    email = shared_state["email"]
    remote = shared_state["remote"]
    creds = shared_state["creds"]
    acc_idx = shared_state["acc_idx"]

    if not dry_run:
        usage = get_storage_usage(remote)
        if usage >= 90:
            if switch_account(acc_idx, email, usage, albums_cache, device_name):
                shared_state["should_restart"] = True
                return {"type": "restart", "item": item}
            else:
                logger.error("Stopping due to full storage and no backup accounts.")
                shared_state["should_restart"] = True
                return {"type": "stop"}

    trip_info = get_assigned_album(filepath, active_trips)
    album_id = None
    album_name = None

    if trip_info:
        album_name = trip_info.get("name")

    if dry_run:
        logger.info(f"🏜️ [DRY RUN] Would upload: {file} ({filesize/1024/1024:.2f} MB) -> Album: {album_name}")
        item["status"] = "dry_run"
        item["album_name"] = album_name
        return item

    if trip_info:
        saved_album_id = trip_info.get("album_id")
        logger.info(f"🎯 Sorting into Album: {album_name}")
        album_id, new_saved_id = get_or_create_album(creds, album_name, db, email, accounts, albums_cache, saved_album_id)

        if new_saved_id and new_saved_id != saved_album_id:
            for t in active_trips:
                if t["name"] == album_name:
                    t["album_id"] = new_saved_id
                    break

    logger.info(f"📤 Uploading: {file} ({filesize/1024/1024:.2f} MB)")
    success, media_id = upload_file_to_google(creds, filepath, album_id, email=email)

    if success:
        logger.info(f"✅ Success: {file}")
        item["status"] = "success"
        item["album_name"] = album_name
        item["remote_id"] = media_id
        return item
    else:
        logger.error(f"❌ Upload Failed: {file}")
        return None
