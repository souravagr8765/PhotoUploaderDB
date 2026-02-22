import os
import sqlite3
import logging
import requests
import json
from datetime import datetime
from dotenv import load_dotenv

# Load env variables
load_dotenv()

# Logger setup specific to database operations
logger = logging.getLogger("Database")
logger.setLevel(logging.INFO)


class DatabaseManager:
    def __init__(self, use_local_cache=False):
        self.url = os.getenv("SUPABASE_URL")
        self.key = os.getenv("SUPABASE_KEY")
        
        if not self.url or not self.key:
            raise ValueError("❌ Missing Supabase credentials in .env file!")
            
        # Ensure URL doesn't end with slash for cleaner concatenation
        self.base_url = self.url.rstrip("/") + "/rest/v1"
        self.table_name = "media_library"
        
        # Headers for PostgREST
        self.headers = {
            "apikey": self.key,
            "Authorization": f"Bearer {self.key}",
            "Content-Type": "application/json",
            "Prefer": "return=representation" 
        }
        
        # Local Cache
        self.cache_conn = None
        self.cache_cursor = None
        if use_local_cache:
            self.init_local_cache()

    def init_local_cache(self):
        """Initializes a transient local SQLite connection for fast lookups."""
        try:
            # We use a file-based DB for persistence across restarts if needed, 
            # but user said 'fetch in starting', implying a fresh sync or update.
            # Let's use a persistent file 'local_cache.db' to avoid re-downloading everything every time?
            # User said: "fetch the databse from the supabase in the starting and create a local database"
            # It's safer to have a persistent cache and just "sync" (fetch new rows).
            # For simplicity v1: Download all (or rely on backup?).
            # Let's look at `backup_to_local_sqlite`. It does exactly this.
            # We can just use the backup file as the read cache!
            
            cache_path = os.path.join(os.path.dirname(__file__), "Data", "local_cache.db")
            os.makedirs(os.path.dirname(cache_path), exist_ok=True)
            self.cache_conn = sqlite3.connect(cache_path)
            self.cache_cursor = self.cache_conn.cursor()
            
            # Ensure table exists locally
            self.cache_cursor.execute("""
                CREATE TABLE IF NOT EXISTS media_library (
                    id TEXT PRIMARY KEY,
                    file_hash TEXT,
                    filename TEXT,
                    file_size_bytes INTEGER,
                    upload_date TEXT,
                    account_email TEXT,
                    device_source TEXT,
                    remote_id TEXT,
                    album_name TEXT,
                    thumbid TEXT
                )
            """)
            self.cache_cursor.execute("CREATE INDEX IF NOT EXISTS idx_filename ON media_library(filename)")
            self.cache_cursor.execute("CREATE INDEX IF NOT EXISTS idx_hash ON media_library(file_hash)")
            
            # Migration check: Add column if missing (for existing users)
            try:
                self.cache_cursor.execute("ALTER TABLE media_library ADD COLUMN album_name TEXT")
            except sqlite3.OperationalError:
                pass # Column likely exists
                
            try:
                self.cache_cursor.execute("ALTER TABLE media_library ADD COLUMN thumbid TEXT")
            except sqlite3.OperationalError:
                pass # Column likely exists
                
            # --- TRIPS CONFIG TABLE ---
            self.cache_cursor.execute("""
                CREATE TABLE IF NOT EXISTS trips_config (
                    name TEXT PRIMARY KEY,
                    start TEXT,
                    end TEXT,
                    require_gps BOOLEAN,
                    album_id TEXT
                )
            """)
                
            self.cache_conn.commit()
            logger.info(f"✅ Local Cache initialized at {cache_path}")
            
        except Exception as e:
            logger.error(f"❌ Failed to init local cache: {e}")

    def sync_cloud_to_local(self):
        """Downloads ALL data from Cloud to Local Cache. 
        Optimization: In future, only download 'new' rows since last sync.
        Current: Full Sync (simplest for now)."""
        if not self.cache_conn: return
        
        logger.info("🔄 Syncing Cloud DB to Local Cache...")
        try:
             # Reuse the backup logic but target our open connection
             # Pagination logic
             endpoint = f"{self.base_url}/{self.table_name}"
             page_size = 1000
             start = 0
             total_synced = 0
             
             # Optimization: Get max ID or count locally to see if we can delta sync?
             # For now, let's just REPLACE INTO.
             
             while True:
                batch_headers = self.headers.copy()
                batch_headers["Range"] = f"{start}-{start + page_size - 1}"
                
                resp = requests.get(endpoint, headers=batch_headers, timeout=30)
                if resp.status_code not in [200, 206]:
                    logger.error(f"Sync error: {resp.text}")
                    break
                
                rows = resp.json()
                if not rows: break
                
                # Insert into local cache
                for row in rows:
                    self.cache_cursor.execute("""
                        REPLACE INTO media_library 
                        (id, file_hash, filename, file_size_bytes, upload_date, account_email, device_source, remote_id, album_name, thumbid)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        row.get('id'), row.get('file_hash'), row.get('filename'), 
                        row.get('file_size_bytes'), row.get('upload_date'), 
                        row.get('account_email'), row.get('device_source'), 
                        row.get('remote_id'), row.get('album_name'), row.get('thumbid')
                    ))
                
                self.cache_conn.commit()
                total_synced += len(rows)
                if len(rows) < page_size: break
                start += page_size
                
             logger.info(f"✅ Sync Complete. {total_synced} records in local cache.")
             
             # --- SYNC TRIPS CONFIG ---
             logger.info("🔄 Syncing Trips Config to Local Cache...")
             trips_endpoint = f"{self.base_url}/trips_config"
             resp_trips = requests.get(trips_endpoint, headers=self.headers, timeout=30)
             if resp_trips.status_code == 200:
                 trips = resp_trips.json()
                 for trip in trips:
                     self.cache_cursor.execute("""
                         REPLACE INTO trips_config (name, start, end, require_gps, album_id)
                         VALUES (?, ?, ?, ?, ?)
                     """, (trip.get('name'), trip.get('start'), trip.get('end'), trip.get('require_gps'), trip.get('album_id')))
                 self.cache_conn.commit()
                 logger.info(f"✅ Trips Sync Complete. {len(trips)} records in local cache.")
             else:
                 logger.error(f"Trips Sync error: {resp_trips.text}")
             
        except Exception as e:
            logger.error(f"❌ Sync failed: {e}")

    def file_exists_by_name(self, filename: str) -> bool:
        """Phase 1: Local Cache Check."""
        if self.cache_cursor:
            try:
                self.cache_cursor.execute("SELECT 1 FROM media_library WHERE filename = ? LIMIT 1", (filename,))
                return self.cache_cursor.fetchone() is not None
            except Exception as e:
                logger.error(f"Local Cache Error: {e}")
                # Fallback to cloud? Or fail safe?
                pass
        
        # Fallback to Cloud (REST)
        return self._cloud_file_exists("filename", filename)

    def file_exists_by_hash(self, file_hash: str) -> bool:
        """Phase 2: Local Cache Check."""
        if self.cache_cursor:
            try:
                self.cache_cursor.execute("SELECT 1 FROM media_library WHERE file_hash = ? LIMIT 1", (file_hash,))
                return self.cache_cursor.fetchone() is not None
            except Exception as e:
                logger.error(f"Local Cache Error: {e}")
                pass
                
        return self._cloud_file_exists("file_hash", file_hash)

    def _cloud_file_exists(self, col, val):
        """Helper for raw cloud check"""
        try:
            endpoint = f"{self.base_url}/{self.table_name}"
            params = {col: f"eq.{val}", "select": "id", "limit": "1"}
            resp = requests.get(endpoint, headers=self.headers, params=params, timeout=10)
            if resp.status_code == 200 and len(resp.json()) > 0:
                return True
            return False
        except: return False


    
    def check_connection(self):
        """Verifies connection to Supabase via REST API."""
        try:
            # Simple query: GET /media_library?select=id&limit=1
            endpoint = f"{self.base_url}/{self.table_name}"
            params = {"select": "id", "limit": "1"}
            
            resp = requests.get(endpoint, headers=self.headers, params=params, timeout=10)
            
            if resp.status_code in [200, 201, 204, 206]:
                logger.info("✅ Connected to Supabase (REST).")
                return True
            else:
                logger.error(f"❌ Connection failed: HTTP {resp.status_code} - {resp.text}")
                return False
        except Exception as e:
            logger.error(f"❌ Connection error: {e}")
            return False


    def insert_file(self, file_data: dict):
        """Inserts a new file record to Cloud AND updates Local Cache."""
        try:
            # 1. Cloud Insert
            endpoint = f"{self.base_url}/{self.table_name}"
            resp = requests.post(endpoint, headers=self.headers, json=file_data, timeout=10)
            
            if resp.status_code not in [200, 201, 204]:
                 logger.error(f"❌ Failed to insert {file_data.get('filename')}: {resp.text}")
                 raise Exception(f"HTTP {resp.status_code}: {resp.text}")
            
            # 2. Local Cache Update
            if self.cache_cursor:
                # We need the ID if possible, but for deduplication we mostly need hash/name.
                # If Supabase returned data (Prefer: return=representation), use it.
                data = resp.json() if resp.status_code in [200, 201] and resp.text else [file_data]
                row = data[0] if isinstance(data, list) and data else file_data
                
                self.cache_cursor.execute("""
                    INSERT OR IGNORE INTO media_library 
                    (id, file_hash, filename, file_size_bytes, upload_date, account_email, device_source, remote_id, album_name, thumbid)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    row.get('id'), row.get('file_hash'), row.get('filename'), 
                    row.get('file_size_bytes'), row.get('upload_date'), 
                    row.get('account_email'), row.get('device_source'), 
                    row.get('remote_id'), row.get('album_name'), row.get('thumbid')
                ))
                self.cache_conn.commit()

        except Exception as e:
            logger.error(f"❌ Failed to insert {file_data.get('filename')}: {e}")
            raise e

    def get_trips(self):
        """Fetches all active trips from the local SQLite cache."""
        trips = []
        if self.cache_cursor:
            try:
                self.cache_cursor.execute("SELECT name, start, end, require_gps, album_id FROM trips_config")
                rows = self.cache_cursor.fetchall()
                for row in rows:
                    trips.append({
                        "name": row[0],
                        "start": row[1],
                        "end": row[2],
                        "require_gps": bool(row[3]),
                        "album_id": row[4]
                    })
            except Exception as e:
                logger.error(f"❌ Failed to fetch trips locally: {e}")
        return trips

    def update_trip_album_id(self, trip_name: str, album_id: str):
        """Updates the album ID for a specific trip in both Cloud and Local Cache."""
        try:
            # 1. Cloud Patch
            endpoint = f"{self.base_url}/trips_config"
            params = {"name": f"eq.{trip_name}"}
            resp = requests.patch(endpoint, headers=self.headers, params=params, json={"album_id": album_id}, timeout=10)
            
            if resp.status_code not in [200, 204]:
                logger.error(f"❌ Failed to update cloud trip {trip_name}: {resp.text}")
            
            # 2. Local Update
            if self.cache_cursor:
                self.cache_cursor.execute("""
                    UPDATE trips_config 
                    SET album_id = ? 
                    WHERE name = ?
                """, (album_id, trip_name))
                self.cache_conn.commit()
                logger.info(f"💾 Updated Album ID for trip '{trip_name}' in cache & cloud.")
                
        except Exception as e:
            logger.error(f"❌ Failed to update trip album ID: {e}")

    def backup_to_local_sqlite(self, backup_path: str):
        """Backs up the entire cloud table to a local SQLite database."""
        try:
            logger.info("💾 Starting local backup...")
            all_rows = []
            
            # PostgREST Pagination via Range Header
            page_size = 1000
            start = 0
            endpoint = f"{self.base_url}/{self.table_name}"
            
            while True:
                # Header: Range: 0-999
                batch_headers = self.headers.copy()
                batch_headers["Range"] = f"{start}-{start + page_size - 1}"
                
                resp = requests.get(endpoint, headers=batch_headers, timeout=30)
                
                if resp.status_code not in [200, 206]:
                    logger.error(f"Error downloading backup batch: {resp.text}")
                    break
                    
                rows = resp.json()
                if not rows:
                    break
                    
                all_rows.extend(rows)
                
                # If we got fewer rows than requested, we're done
                if len(rows) < page_size:
                    break
                    
                start += page_size

            if not all_rows:
                logger.info("ℹ️ Database is empty, nothing to backup.")
                return

            # Save to SQLite (Standard Logic)
            os.makedirs(os.path.dirname(backup_path), exist_ok=True)
            conn = sqlite3.connect(backup_path)
            cursor = conn.cursor()

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS media_library (
                    id TEXT PRIMARY KEY,
                    file_hash TEXT,
                    filename TEXT,
                    file_size_bytes INTEGER,
                    upload_date TEXT,
                    account_email TEXT,
                    device_source TEXT,
                    remote_id TEXT,
                    album_name TEXT,
                    thumbid TEXT
                )
            """)
            
            # Migration check: Add column if missing (for backups too)
            try:
                cursor.execute("ALTER TABLE media_library ADD COLUMN album_name TEXT")
            except sqlite3.OperationalError:
                pass 
            
            try:
                cursor.execute("ALTER TABLE media_library ADD COLUMN thumbid TEXT")
            except sqlite3.OperationalError:
                pass

            count = 0
            for row in all_rows:
                cursor.execute("""
                    REPLACE INTO media_library 
                    (id, file_hash, filename, file_size_bytes, upload_date, account_email, device_source, remote_id, album_name, thumbid)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    row.get('id'), row.get('file_hash'), row.get('filename'), 
                    row.get('file_size_bytes'), row.get('upload_date'), 
                    row.get('account_email'), row.get('device_source'), 
                    row.get('remote_id'), row.get('album_name'), row.get('thumbid')
                ))
                count += 1
                
            logger.info(f"✅ Backup complete: {count} records saved to {backup_path}")
            
            # --- BACKUP TRIPS CONFIG ---
            try:
                # Get all trips dynamically from the same source
                trips_endpoint = f"{self.base_url}/trips_config"
                resp_trips = requests.get(trips_endpoint, headers=self.headers, timeout=30)
                if resp_trips.status_code == 200:
                    trips = resp_trips.json()
                    
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS trips_config (
                            name TEXT PRIMARY KEY,
                            start TEXT,
                            end TEXT,
                            require_gps BOOLEAN,
                            album_id TEXT
                        )
                    """)
                    
                    for trip in trips:
                        cursor.execute("""
                            REPLACE INTO trips_config (name, start, end, require_gps, album_id)
                            VALUES (?, ?, ?, ?, ?)
                        """, (trip.get('name'), trip.get('start'), trip.get('end'), trip.get('require_gps'), trip.get('album_id')))
                        
                    logger.info(f"✅ Backup Trips complete: {len(trips)} records saved to {backup_path}")
                else:
                    logger.error(f"Error downloading trips backup: {resp_trips.text}")
            except Exception as e:
                logger.error(f"❌ Tripps backup failed: {e}")

            conn.commit()
            conn.close()

        except Exception as e:
            logger.error(f"❌ Backup failed: {e}")

if __name__ == "__main__":
    # Test script
    logging.basicConfig(level=logging.INFO)
    try:
        db = DatabaseManager()
        if db.check_connection():
            print("Database connection successful (using requests).")
    except Exception as e:
        print(f"Init Error: {e}")
