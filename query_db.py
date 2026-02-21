import os
import json
import logging
from database import DatabaseManager

# Setup Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("QueryTool")

def run_query():
    print("🚀 Supabase Query Tool")
    print("----------------------")
    
    try:
        db = DatabaseManager(use_local_cache=False) # Always query cloud for this tool
        print("✅ Connected to Supabase (REST API)")
    except Exception as e:
        print(f"❌ Connection Failed: {e}")
        return

    while True:
        print("\nOptions:")
        print("1. Count Total Records")
        print("2. Search by Filename")
        print("3. Search by Album Name")
        print("4. Custom Filter (Advanced)")
        print("5. Exit")
        
        choice = input("\nEnter choice (1-5): ").strip()
        
        if choice == "1":
            # HEAD request to get count
            endpoint = f"{db.base_url}/{db.table_name}"
            try:
                # Prefer: count=exact header
                headers = db.headers.copy()
                headers["Prefer"] = "count=exact"
                resp = requests.get(endpoint, headers=headers, params={"select": "id", "limit": "1"})
                
                # Content-Range: 0-0/1234
                content_range = resp.headers.get("Content-Range")
                if content_range:
                    total = content_range.split("/")[-1]
                    print(f"\n📊 Total Records: {total}")
                else:
                    print("\n⚠️ Could not determine count.")
            except Exception as e:
                print(f"Error: {e}")

        elif choice == "2":
            filename = input("Enter filename (or part of it): ").strip()
            # GET /media_library?filename=ilike.*foo*
            endpoint = f"{db.base_url}/{db.table_name}"
            params = {
                "filename": f"ilike.*{filename}*",
                "select": "*"
            }
            _exec_request(db, params)

        elif choice == "3":
            album = input("Enter album name: ").strip()
            # GET /media_library?album_name=eq.Foo
            endpoint = f"{db.base_url}/{db.table_name}"
            params = {
                "album_name": f"eq.{album}",
                "select": "*"
            }
            _exec_request(db, params)
            
        elif choice == "4":
            print("\nEnter a PostgREST filter string (e.g., file_size_bytes=gt.1000000)")
            # Users need to know PostgREST syntax
            query = input("Query: ").strip()
            if not query: continue
            
            # Simple parsing: key=op.val
            try:
                base_params = {"select": "*"}
                # Manually appending to URL might be easier for complex queries, 
                # but let's try to inject as params if possible.
                # Actually, `requests` params dict handles this: {"col": "eq.val"}
                
                # Split by & if multiple
                parts = query.split("&")
                endpoint = f"{db.base_url}/{db.table_name}"
                params = base_params.copy()
                
                for part in parts:
                    if "=" in part:
                        k, v = part.split("=", 1)
                        params[k] = v
                
                _exec_request(db, params)
            except Exception as e:
                print(f"Parsing Error: {e}")

        elif choice == "5":
            print("👋 Bye!")
            break

import requests

def _exec_request(db, params):
    try:
        endpoint = f"{db.base_url}/{db.table_name}"
        resp = requests.get(endpoint, headers=db.headers, params=params)
        
        if resp.status_code == 200:
            data = resp.json()
            print(f"\n✅ Found {len(data)} records:\n")
            print(json.dumps(data, indent=2))
        else:
            print(f"❌ Error {resp.status_code}: {resp.text}")
    except Exception as e:
        print(f"❌ Request Failed: {e}")

if __name__ == "__main__":
    run_query()
