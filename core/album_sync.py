from concurrent.futures import ThreadPoolExecutor
import requests
import json
import infra.logger as logger

def fetch_album_media_ids(creds, album_id):
    """
    Fetches all media item IDs in a Google Photos album.
    Handles pagination. Uses field masking for speed.
    """
    media_ids = set()
    # Adding fields parameter to only fetch what we need (ID and pagination token)
    url = 'https://photoslibrary.googleapis.com/v1/mediaItems:search?fields=mediaItems/id,nextPageToken'
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
    Uses high-performance batch updates.
    """
    album_name = trip['name']
    raw_album_id = trip.get('album_id')
    
    if not raw_album_id:
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
        album_id = raw_album_id

    if not album_id:
        return
        
    import time
    start_fetch = time.time()
    logger.info(f"🔄 Syncing removals for album: {album_name}")
    
    # 1. Fetch current IDs from Google Photos (100% accurate full fetch)
    google_ids = fetch_album_media_ids(creds, album_id)
    fetch_time = time.time() - start_fetch
    
    if google_ids is None:
        logger.warning(f"⚠️ Skipping sync for '{album_name}' due to API fetch failure.")
        return
    
    logger.info(f"⏱️ Fetched {len(google_ids)} IDs from Google in {fetch_time:.2f}s")
    
    # 2. Fetch IDs from DB
    start_db = time.time()
    db_ids = db.get_album_remote_ids(album_name, current_email)
    db_time = time.time() - start_db
    logger.info(f"⏱️ Fetched {len(db_ids)} IDs from DB in {db_time:.2f}s")
    
    # 3. Identify changes and batch update
    to_remove = [remote_id for remote_id in db_ids if remote_id not in google_ids]
    to_adopt = [remote_id for remote_id in google_ids if remote_id not in db_ids]
    
    # Process Removals
    if to_remove:
        logger.info(f"🗑️ Found {len(to_remove)} items removed from Google Photos in album '{album_name}'. Updating DB...")
        db.remove_photos_from_album_batch(to_remove, album_name)
    
    # Process Adoptions (Matching by remote_id)
    if to_adopt:
        logger.info(f"🔗 Found {len(to_adopt)} items in Google album '{album_name}' but missing in DB trip link. Linking...")
        db.adopt_photos_to_album_batch(to_adopt, album_name)

    if not to_remove and not to_adopt:
        logger.debug(f"✨ Album '{album_name}' is already in sync.")
    else:
        logger.info(f"✅ Successfully synced changes for album '{album_name}'.")

def sync_all_trips(db, creds, active_trips, current_email):
    """
    Iterates through all provided trips and synchronizes album removals in parallel.
    """
    if not active_trips:
        return

    logger.info("="*50)
    logger.info(f"🔄 Starting Parallel Album Sync for {len(active_trips)} trips...")
    
    # Use ThreadPoolExecutor to fetch and sync multiple albums at once
    # Max workers set to 5 to avoid overwhelming the Google Photos API rate limits
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = []
        for trip in active_trips:
            futures.append(executor.submit(sync_album_removals, db, creds, trip, current_email))
            
        # Wait for all to complete
        for future in futures:
            try:
                future.result()
            except Exception as e:
                logger.error(f"❌ Trip sync thread failed: {e}")

    logger.info("🔄 Album Synchronization Complete.")
    logger.info("="*50)
