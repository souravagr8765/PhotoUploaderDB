import os
import sys
import json
import requests
import pickle

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.balancer import DatabaseManager
from infra.auth import get_creds, ACCOUNTS
import infra.logger as logger

def update_missing_urls():
    print("🚀 Starting Album URL Repair Script")
    db = DatabaseManager(use_local_cache=False)
    trips = db.get_trips()
    
    updated_count = 0
    
    for trip in trips:
        name = trip['name']
        album_id_str = trip['album_id']
        album_url = trip['album_url']
        
        if album_id_str and not album_url:
            print(f"🔍 Trip '{name}' is missing album URL. Attempting to fetch...")
            
            # Parse album_id
            album_dict = {}
            if album_id_str.strip().startswith("{") and album_id_str.strip().endswith("}"):
                try:
                    album_dict = json.loads(album_id_str)
                except:
                    print(f"⚠️ Failed to parse album_id JSON for {name}")
                    continue
            else:
                # Legacy ID (straight string)
                if ACCOUNTS:
                    album_dict = {ACCOUNTS[0]: album_id_str}
                else:
                    print(f"⚠️ No accounts configured to check legacy ID for {name}")
                    continue
            
            found_url = None
            for email, aid in album_dict.items():
                if email == "legacy_creator":
                    if ACCOUNTS:
                        email = ACCOUNTS[0]
                    else:
                        continue
                
                print(f"  Trying account: {email} for ID: {aid}...")
                creds = get_creds(email)
                if not creds:
                    print(f"  ⚠️ No credentials found for {email}. Make sure token exists in creds/ folder.")
                    continue
                
                headers = {
                    'Authorization': f'Bearer {creds.token}',
                    'Content-type': 'application/json'
                }
                
                try:
                    resp = requests.get(f'https://photoslibrary.googleapis.com/v1/albums/{aid}', headers=headers, timeout=30)
                    if resp.status_code == 200:
                        found_url = resp.json().get("productUrl")
                        if found_url:
                            print(f"  ✅ Found URL: {found_url}")
                            break
                    else:
                        print(f"  ❌ API returned {resp.status_code}: {resp.text}")
                except Exception as e:
                    print(f"  ❌ Error fetching from API: {e}")
            
            if found_url:
                db.update_trip_album_id(name, album_id_str, found_url)
                print(f"✅ Updated Database for '{name}'")
                updated_count += 1
            else:
                print(f"❌ Could not retrieve URL for trip '{name}' from any associated account.")
                
    print(f"\n✨ Finished. Updated {updated_count} trips.")

if __name__ == "__main__":
    update_missing_urls()
