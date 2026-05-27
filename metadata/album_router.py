import os
import json
import requests
import infra.logger as logger
from metadata.extractor import get_photo_metadata
from infra.auth import wait_for_internet
from infra.notifications import send_notification

def get_assigned_album(filepath, active_trips):
    """
    Determines if a photo belongs to a configured trip based on metadata.
    Returns: Trip dictionary or None.
    """
    date_obj, has_gps = get_photo_metadata(filepath)
    if not date_obj: return None
    
    date_str = date_obj.strftime("%Y-%m-%d") # Compare just dates
    is_video = not filepath.lower().endswith(('.jpg', '.jpeg', '.heic', '.png', '.webp', '.bmp', '.gif'))
    
    for trip in active_trips:
        # Check Date Range
        if trip["start"] <= date_str <= trip["end"]:
            # Check GPS Constraint (ignore for videos)
            if not is_video and trip.get("require_gps", False) and not has_gps:
                continue
                
            logger.info(f"🎯 Matched Album: {trip['name']} for {os.path.basename(filepath)}")
            return trip
            
    return None

def get_or_create_album(creds, album_name, db, email, accounts, albums_cache, saved_album_id=None, saved_album_url=None):
    """
    Checks if album exists (via cache or API). If not, creates it.
    Returns: (album_id_for_upload, updated_db_album_id_string)
    """
    if not album_name: return None, None
    
    # 1. Check Runtime Cache (most efficient)
    if album_name in albums_cache:
        return albums_cache[album_name], None

    album_dict = None
    found_album_id = None
    
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
                found_album_id = album_dict[email]
        else:
            album_dict = {"legacy_creator": saved_album_id}
            if len(accounts) > 0 and email == accounts[0]:
                found_album_id = saved_album_id

        # If we have an ID AND a URL already, we can populate cache and return immediately
        if found_album_id and saved_album_url:
            albums_cache[album_name] = found_album_id
            return found_album_id, saved_album_id

    # If we are here, we either don't have an ID for this account OR we have an ID but no URL.
    # We proceed to use the API.
    wait_for_internet()

    headers = {
        'Authorization': f'Bearer {creds.token}',
        'Content-type': 'application/json'
    }

    try:
        # 2. Search for existing album by name via API (only if we don't already have an ID)
        page_token = None
        while not found_album_id:
            params = {"pageSize": 50}
            if page_token:
                params["pageToken"] = page_token
            list_resp = requests.get(
                'https://photoslibrary.googleapis.com/v1/albums',
                headers=headers, params=params, timeout=30
            )
            if list_resp.status_code != 200:
                logger.warning(f"⚠️ Could not list albums (status {list_resp.status_code}). Will proceed to create.")
                break
            list_data = list_resp.json()
            for album in list_data.get("albums", []):
                if album.get("title", "").lower() == album_name.lower():
                    found_album_id = album.get("id")
                    break
            
            page_token = list_data.get("nextPageToken")
            if not page_token or found_album_id:
                break

        if found_album_id:
            logger.info(f"📁 Found existing album '{album_name}' (ID: {found_album_id}). Reusing.")
            albums_cache[album_name] = found_album_id
            if album_dict is None:
                album_dict = {}
            album_dict[email] = found_album_id
            new_saved_id = json.dumps(album_dict)
            # Fetch album details to get the URL
            album_url = "https://photos.google.com/albums" # Default
            try:
                album_resp = requests.get(f'https://photoslibrary.googleapis.com/v1/albums/{found_album_id}', headers=headers, timeout=30)
                if album_resp.status_code == 200:
                    album_url = album_resp.json().get("productUrl", album_url)
            except: pass
            
            db.update_trip_album_id(album_name, new_saved_id, album_url)
            return found_album_id, new_saved_id

        # 3. Album not found — create it
        payload = {"album": {"title": album_name}}
        resp = requests.post('https://photoslibrary.googleapis.com/v1/albums', headers=headers, json=payload, timeout=30)

        if resp.status_code == 200:
            data = resp.json()
            album_id = data.get("id")
            album_url = data.get("productUrl", "https://photos.google.com/albums")
            albums_cache[album_name] = album_id

            if album_dict is None:
                album_dict = {}
            album_dict[email] = album_id

            new_saved_id = json.dumps(album_dict)

            # Save to persistent database
            db.update_trip_album_id(album_name, new_saved_id, album_url)

            logger.info(f"📁 Created Album '{album_name}' for account {email}")

            if len(album_dict) > 1:
                subject = f"🔔 Album Split Notification: {album_name}"
                body = (f"Storage was full, so a NEW part of the album '{album_name}' "
                        f"was created on account: {email}.\n\n"
                        f"🔗 Link to album: {album_url}\n\n"
                        f"IMPORTANT: Please open the link above for {email} and manually share "
                        f"this album with your main account to merge them together!")
                msg_id = send_notification(subject, body)
            else:
                subject = f"📸 New Trip Album Created: {album_name}"
                body = (f"A brand new album was created for trip '{album_name}' "
                        f"on account: {email}.\n\n"
                        f"🔗 Link to album: {album_url}")
                msg_id = send_notification(subject, body)

            if msg_id:
                try:
                    db.update_trip_message_id(album_name, msg_id)
                    logger.info(f"📧 Saved email Message-ID for trip '{album_name}'")
                except Exception as e:
                    logger.warning(f"⚠️ Could not save email Message-ID for trip '{album_name}': {e}")

            return album_id, new_saved_id
        else:
            logger.error(f"Failed to create album {album_name}: {resp.text}")
            return None, None
    except Exception as e:
        logger.error(f"Album API Error: {e}")
        return None, None
