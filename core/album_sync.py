import requests
import json
import infra.logger as logger

def fetch_album_media_ids(creds, album_id):
    """
    Fetches all media item IDs in a Google Photos album.
    Handles pagination.
    """
    media_ids = set()
    url = 'https://photoslibrary.googleapis.com/v1/mediaItems:search'
    headers = {
        'Authorization': f'Bearer {creds.token}',
        'Content-type': 'application/json'
    }
    
    payload = {
        "albumId": album_id,
        "pageSize": 100
    }
    
    while True:
        try:
            response = requests.post(url, headers=headers, json=payload, timeout=30)
            if response.status_code != 200:
                logger.error(f"Failed to fetch media items for album {album_id}: {response.text}")
                return None  # Return None to signal failure
                
            data = response.json()
            items = data.get('mediaItems', [])
            for item in items:
                media_ids.add(item['id'])
                
            next_token = data.get('nextPageToken')
            if not next_token:
                break
            payload['pageToken'] = next_token
        except Exception as e:
            logger.error(f"Error fetching media items for album {album_id}: {e}")
            return None # Return None to signal failure
            
    return media_ids

def sync_album_removals(db, creds, trip, current_email):
    """
    Compares the photos in a Google Photos album with the database records.
    If a photo is in the DB but NOT in the Google Photos album, it removes the album link in the DB.
    """
    album_name = trip['name']
    raw_album_id = trip.get('album_id')
    
    if not raw_album_id:
        logger.info(f"Skipping sync for trip '{album_name}' (No Album ID found).")
        return

    # Resolve album_id if it's a JSON map (for multi-account support)
    album_id = None
    try:
        if raw_album_id.strip().startswith("{"):
            id_map = json.loads(raw_album_id)
            album_id = id_map.get(current_email)
        else:
            album_id = raw_album_id
    except Exception as e:
        logger.warning(f"⚠️ Could not parse album ID map for '{album_name}': {e}")
        album_id = raw_album_id

    if not album_id:
        logger.info(f"Skipping sync for trip '{album_name}' (No ID for current account {current_email}).")
        return
        
    logger.info(f"🔄 Syncing removals for album: {album_name}")
    
    # 1. Fetch current IDs from Google Photos
    google_ids = fetch_album_media_ids(creds, album_id)
    
    if google_ids is None:
        logger.warning(f"⚠️ Skipping sync for '{album_name}' due to API fetch failure (check scopes).")
        return
    
    # 2. Fetch IDs from DB
    db_ids = db.get_album_remote_ids(album_name, current_email)
    
    # 3. Identify removals
    removed_count = 0
    for remote_id in db_ids:
        if remote_id not in google_ids:
            # Note: We only log and remove the association.
            # This doesn't delete the photo from Google Photos or local storage.
            logger.info(f"🗑️ Detected removal from Google Photos: {remote_id} (was in album: {album_name})")
            db.remove_photo_from_album_record(remote_id, album_name)
            removed_count += 1
            
    if removed_count > 0:
        logger.info(f"✅ Successfully synced {removed_count} removals for album '{album_name}'.")
    else:
        logger.info(f"✨ Album '{album_name}' is already in sync.")

def sync_all_trips(db, creds, active_trips, current_email):
    """
    Iterates through all provided trips and synchronizes album removals.
    """
    if not active_trips:
        return

    logger.info("="*50)
    logger.info("🔄 Starting Album Removal Synchronization...")
    for trip in active_trips:
        sync_album_removals(db, creds, trip, current_email)
    logger.info("🔄 Album Removal Synchronization Complete.")
    logger.info("="*50)
