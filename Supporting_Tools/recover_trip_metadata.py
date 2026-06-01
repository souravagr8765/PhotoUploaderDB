import os
import sys
import json
import requests
from datetime import datetime

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.balancer import DatabaseManager
from infra.auth import get_creds, ACCOUNTS
import infra.logger as logger

def recover_missing_trips():
    print("🚀 Starting Trip Metadata Recovery Script")
    print("This script will search Google Photos for albums matching your trip names to restore missing IDs.")
    
    db = DatabaseManager(use_local_cache=False)
    trips = db.get_trips()
    
    recovered_count = 0
    
    for trip in trips:
        name = trip['name']
        album_id = trip['album_id']
        
        # We consider it "missing" if album_id is empty, null, or a default empty JSON
        is_missing = not album_id or album_id == "{}" or album_id == "null"
        
        if is_missing:
            print(f"\n🔍 Trip '{name}' is missing metadata. Searching Google Photos...")
            
            found_album_id = None
            found_album_url = None
            found_email = None
            
            for email in ACCOUNTS:
                print(f"  Searching account: {email}...")
                creds = get_creds(email)
                if not creds:
                    continue
                
                headers = {
                    'Authorization': f'Bearer {creds.token}',
                    'Content-type': 'application/json'
                }
                
                # List albums and search for name match
                try:
                    # We might need to paginate if there are many albums
                    next_page_token = None
                    while True:
                        url = 'https://photoslibrary.googleapis.com/v1/albums?pageSize=50'
                        if next_page_token:
                            url += f'&pageToken={next_page_token}'
                            
                        resp = requests.get(url, headers=headers, timeout=30)
                        if resp.status_code != 200:
                            print(f"    ❌ API Error: {resp.status_code}")
                            break
                            
                        data = resp.json()
                        albums = data.get('albums', [])
                        
                        for album in albums:
                            if album.get('title') == name:
                                found_album_id = album.get('id')
                                found_album_url = album.get('productUrl')
                                found_email = email
                                break
                        
                        if found_album_id: break
                        
                        next_page_token = data.get('nextPageToken')
                        if not next_page_token: break
                        
                except Exception as e:
                    print(f"    ❌ Error searching account: {e}")
                
                if found_album_id:
                    break
            
            if found_album_id:
                print(f"  ✅ Found Match! ID: {found_album_id[:10]}... on {found_email}")
                # Construct the album_id JSON
                id_json = json.dumps({found_email: found_album_id})
                db.update_trip_album_id(name, id_json, found_album_url)
                print(f"  💾 Database updated for '{name}'.")
                recovered_count += 1
            else:
                print(f"  ❌ No matching album found in any account for '{name}'.")
                
    print(f"\n✨ Recovery finished. Restored {recovered_count} trips.")
    if recovered_count > 0:
        print("Run the uploader normally now to resume syncing.")

if __name__ == "__main__":
    recover_missing_trips()
