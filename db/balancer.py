import os
import sqlite3
import threading
import logging
import infra.logger as lg
import random
from datetime import datetime
from infra.config_loader import get_config
import urllib.parse
import sys
import json
import queue
import time

import psycopg2
from tqdm import tqdm
from infra.notifications import send_notification as send_notification_email

# Logger setup specific to database operations
logger = lg


# send_notification_email is imported from infra.auth as send_notification_email

class DatabaseBalancer:
    def __init__(self, use_local_cache=False):
        # We assume standard PostgreSQL connection URIs in the config
        self.nhost_url = get_config("database.nhost_url")
        self.neon_url = get_config("database.neon_url")
        self.nhost_enabled = get_config("database.nhost_enabled", True)
        self.neon_enabled = get_config("database.neon_enabled", True)

        if not self.nhost_url and self.nhost_enabled:
            logger.warning("Missing database.nhost_url in config.yaml but Nhost is enabled.")
        if not self.neon_url and self.neon_enabled:
            logger.warning("Missing database.neon_url in config.yaml but Neon is enabled.")

        self.provider_a_active = False
        self.provider_b_active = False

        self.conn_a = None
        self.conn_b = None

        self._sqlite_lock = threading.Lock()  # Protects all SQLite operations across threads

        # --- Background Worker for Stats Updates ---
        self._stats_queue = queue.Queue()
        self._worker_thread = threading.Thread(target=self._stats_worker, daemon=True)
        self._worker_thread.start()

        self._connect_providers()

        self.cache_conn = None
        self.cache_cursor = None
        if use_local_cache:
            self.init_local_cache()
            self.reconcile_databases()

    def _stats_worker(self):
        """Background thread that processes stats updates from the queue."""
        while True:
            try:
                task = self._stats_queue.get()
                if task is None: break # Shutdown signal
                
                func_name, args = task
                try:
                    if func_name == "increment_storage_summary":
                        self._do_increment_storage_summary(*args)
                    elif func_name == "refresh_storage_summary":
                        self._do_refresh_storage_summary(*args)
                except Exception as e:
                    logger.error(f"❌ Background stats update ({func_name}) failed: {e}")
                finally:
                    self._stats_queue.task_done()
            except Exception as e:
                logger.error(f"⚠️ Stats worker thread encountered an error: {e}")
                time.sleep(1)

    def _parse_url(self, url: str):
        if not url: return {}
        parsed = urllib.parse.urlparse(url)
        return {
            "user": parsed.username,
            "password": parsed.password,
            "host": parsed.hostname,
            "port": parsed.port or 5432,
            "database": parsed.path.lstrip('/')
        }

    def _is_connection_error(self, e: Exception) -> bool:
        err_str = str(e).lower()
        if any(x in err_str for x in ["10054", "10053", "forcibly closed", "network error", "broken pipe", "connection reset", "connection aborted", "interfaceerror", "closed"]):
            return True
        if isinstance(e, (ConnectionError, OSError)): return True
        return False

    def _reconnect_provider(self, provider_id: str):
        if provider_id == 'A' and self.nhost_url:
            try:
                # psycopg2 handles postgres:// URIs natively
                self.conn_a = psycopg2.connect(self.nhost_url)
                self.conn_a.autocommit = True
                self.provider_a_active = True
                logger.info("✅ Connected to Nhost (Provider A).")
                return True
            except Exception as e:
                self.provider_a_active = False
                logger.error(f"❌ Nhost Connection/Reconnect Failed: {e}")
                return False
        elif provider_id == 'B' and self.neon_url:
            try:
                # psycopg2 handles postgresql:// URIs natively
                self.conn_b = psycopg2.connect(self.neon_url)
                self.conn_b.autocommit = True
                self.provider_b_active = True
                logger.info("✅ Connected to Neon (Provider B).")
                return True
            except Exception as e:
                self.provider_b_active = False
                logger.error(f"❌ Neon Connection/Reconnect Failed: {e}")
                return False
        return False

    def _migrate_cloud_schema(self):
        """Add new columns and tables to cloud database if they don't exist yet."""
        
        # Current Schema Version - increment this when modifying CLOUD_SCHEMA
        CURRENT_VERSION = 3

        # Comprehensive Cloud Schema Definition (PostgreSQL)
        # This acts as the source of truth for automatic migrations.
        CLOUD_SCHEMA = {
            "schema_info": {
                "id": "INTEGER PRIMARY KEY",
                "version": "INTEGER",
                "updated_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
            },
            "media_library": {
                "sl_no": "SERIAL PRIMARY KEY",
                "file_hash": "TEXT NOT NULL",
                "filename": "TEXT",
                "file_size_bytes": "BIGINT",
                "upload_date": "TEXT",
                "account_email": "TEXT",
                "device_source": "TEXT",
                "remote_id": "TEXT",
                "album_name": "TEXT",
                "updated_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
            },
            "storage_summary": {
                "id": "SERIAL PRIMARY KEY",
                "synced_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
                "total_photos": "INTEGER DEFAULT 0",
                "total_videos": "INTEGER DEFAULT 0",
                "total_assets": "INTEGER DEFAULT 0",
                "total_photos_size_gb": "REAL DEFAULT 0",
                "total_videos_size_gb": "REAL DEFAULT 0",
                "total_size_gb": "REAL DEFAULT 0",
                "updated_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
            },
            "account_distribution": {
                "id": "SERIAL PRIMARY KEY",
                "summary_id": "INTEGER REFERENCES storage_summary(id) ON DELETE CASCADE",
                "account_email": "TEXT NOT NULL",
                "photos_count": "INTEGER DEFAULT 0",
                "videos_count": "INTEGER DEFAULT 0",
                "photos_size_mb": "REAL DEFAULT 0",
                "videos_size_mb": "REAL DEFAULT 0",
                "total_size_mb": "REAL DEFAULT 0",
                "percentage": "REAL DEFAULT 0",
                "updated_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
            },
            "device_distribution": {
                "device_name": "TEXT PRIMARY KEY",
                "id": "SERIAL",
                "summary_id": "INTEGER REFERENCES storage_summary(id) ON DELETE CASCADE",
                "photos_count": "INTEGER DEFAULT 0",
                "videos_count": "INTEGER DEFAULT 0",
                "photos_size_mb": "REAL DEFAULT 0",
                "videos_size_mb": "REAL DEFAULT 0",
                "total_size_mb": "REAL DEFAULT 0",
                "percentage": "REAL DEFAULT 0",
                "updated_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
            },
            "sync_tracker": {
                "id": "INTEGER PRIMARY KEY",
                "last_sync_time": "TIMESTAMP DEFAULT '1970-01-01 00:00:00'"
            },
            "trips_config": {
                "name": "TEXT PRIMARY KEY",
                "sl_no": "SERIAL",
                "start": "TEXT",
                "end": "TEXT",
                "require_gps": "BOOLEAN DEFAULT FALSE",
                "album_id": "TEXT",
                "album_url": "TEXT",
                "email_message_id": "TEXT",
                "asset_metadata": "JSONB",
                "updated_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
            },
            "device_config": {
                "device_name": "TEXT PRIMARY KEY",
                "directories": "TEXT",
                "sl_no": "SERIAL",
                "updated_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
            }
        }

        for active, conn, name in [
            (self.provider_a_active, self.conn_a, "Nhost (A)"),
            (self.provider_b_active, self.conn_b, "Neon (B)")
        ]:
            if not active:
                continue
            try:
                cursor = conn.cursor()

                # 0. Fast-Check Schema Version
                try:
                    cursor.execute("SELECT version FROM schema_info WHERE id = 1")
                    res = cursor.fetchone()
                    if res and res[0] >= CURRENT_VERSION:
                        logger.debug(f"✅ Schema version {res[0]} is up to date on {name}")
                        continue
                except Exception:
                    # Table might not exist, proceed to full migration
                    pass

                logger.info(f"🚀 Running full schema migration on {name} (Version {CURRENT_VERSION})...")
                
                # 1. Automatic Table and Column Creation
                for table, columns in CLOUD_SCHEMA.items():
                    # Get the primary key column to initialize the table
                    pk_col = next(col for col, dtype in columns.items() if "PRIMARY KEY" in dtype)
                    # Quote table and pk_col to handle reserved keywords
                    cursor.execute(f'CREATE TABLE IF NOT EXISTS "{table}" ("{pk_col}" {columns[pk_col]})')
                    
                    # Add all other columns if they don't exist
                    for col, dtype in columns.items():
                        if col == pk_col: continue
                        # Quote col name to handle reserved keywords like "end"
                        cursor.execute(f'ALTER TABLE "{table}" ADD COLUMN IF NOT EXISTS "{col}" {dtype}')

                # 2. Specific Constraints and Cleanup
                
                # media_library UNIQUE constraint
                cursor.execute("""
                    DELETE FROM media_library a USING media_library b
                    WHERE a.sl_no > b.sl_no AND a.file_hash = b.file_hash
                """)
                try:
                    cursor.execute("ALTER TABLE media_library ADD CONSTRAINT media_library_file_hash_unique UNIQUE (file_hash)")
                except Exception: pass
                
                # Ensure storage_summary ID=1 exists
                cursor.execute("INSERT INTO storage_summary (id) VALUES (1) ON CONFLICT (id) DO NOTHING")
                
                # account_distribution UNIQUE constraint
                cursor.execute("""
                    DELETE FROM account_distribution a USING account_distribution b
                    WHERE a.id < b.id AND a.account_email = b.account_email
                """)
                try:
                    cursor.execute("ALTER TABLE account_distribution ADD CONSTRAINT acc_dist_email_unique UNIQUE (account_email)")
                except Exception: pass
                
                # device_distribution UNIQUE constraint
                cursor.execute("""
                    DELETE FROM device_distribution a USING device_distribution b
                    WHERE a.id < b.id AND a.device_name = b.device_name
                """)
                try:
                    cursor.execute("ALTER TABLE device_distribution ADD CONSTRAINT dev_dist_name_unique UNIQUE (device_name)")
                except Exception: pass

                # 3. Performance Indexes
                try:
                    # Functional index for fast extension-based filtering (PostgreSQL)
                    cursor.execute(r"CREATE INDEX IF NOT EXISTS idx_media_library_ext ON media_library (LOWER(SUBSTRING(filename FROM '\.([^\.]+)$')))")
                    # Indexes for distribution groupings
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_media_library_account ON media_library (account_email)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_media_library_device ON media_library (device_source)")
                    # Indexes for album sync performance
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_media_library_album ON media_library (album_name)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_media_library_remote ON media_library (remote_id)")
                except Exception as e:
                    logger.warning(f"⚠️ Could not create performance indexes: {e}")
                
                # 4. Update Schema Version
                cursor.execute("INSERT INTO schema_info (id, version) VALUES (1, %s) ON CONFLICT (id) DO UPDATE SET version = EXCLUDED.version", (CURRENT_VERSION,))
                
                conn.commit()
                
                # 3. PostgreSQL Triggers for updated_at
                try:
                    cursor.execute("""
                        CREATE OR REPLACE FUNCTION update_updated_at_column()
                        RETURNS TRIGGER AS $$
                        BEGIN
                            IF (NEW.updated_at IS NOT DISTINCT FROM OLD.updated_at) THEN
                                NEW.updated_at = CURRENT_TIMESTAMP;
                            END IF;
                            RETURN NEW;
                        END;
                        $$ language 'plpgsql';
                    """)
                    
                    for table in CLOUD_SCHEMA.keys():
                        if table == "sync_tracker": continue
                        cursor.execute(f"""
                            DO $$
                            BEGIN
                                IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'update_{table}_updated_at') THEN
                                    CREATE TRIGGER update_{table}_updated_at
                                    BEFORE UPDATE ON "{table}"
                                    FOR EACH ROW
                                    EXECUTE PROCEDURE update_updated_at_column();
                                END IF;
                            END $$;
                        """)
                    conn.commit()
                    logger.debug(f"✅ Ensured triggers on {name}")
                except Exception as e:
                    logger.warning(f"⚠️ Could not create triggers on {name}: {e}")
                
                logger.debug(f"✅ Ensured schema on {name}")
            except Exception as e:
                logger.warning(f"⚠️ Could not migrate schema on {name}: {e}")

    def refresh_storage_summary(self, use_local_for_calc=True):
        """Recalculates storage statistics using high-performance SQL aggregations and batched writes."""
        logger.info(f"📊 Refreshing storage summary stats (using SQL aggregations)...")
        
        try:
            # 1. Consolidated Data Retrieval
            if not (use_local_for_calc and self.cache_cursor):
                # Cloud Path: Single-scan consolidation using PostgreSQL GROUPING SETS
                sql_consolidated = r"""
                    WITH media_stats AS (
                        SELECT 
                            account_email, 
                            device_source, 
                            album_name, 
                            file_size_bytes,
                            CASE WHEN LOWER(SUBSTRING(filename FROM '\.([^\.]+)$')) IN ('jpg', 'jpeg', 'png', 'gif', 'bmp', 'webp', 'heic', 'heif', 'tiff') THEN 1 ELSE 0 END as is_photo,
                            CASE WHEN LOWER(SUBSTRING(filename FROM '\.([^\.]+)$')) IN ('mp4', 'mov', 'avi', 'mkv', 'wmv', 'flv', 'webm') THEN 1 ELSE 0 END as is_video
                        FROM media_library
                    )
                    SELECT 
                        GROUPING(account_email) as g_acc, 
                        GROUPING(device_source) as g_dev, 
                        GROUPING(album_name) as g_album,
                        account_email, 
                        device_source, 
                        album_name,
                        COUNT(*) as total_assets,
                        SUM(is_photo) as total_photos,
                        SUM(is_video) as total_videos,
                        SUM(CASE WHEN is_photo = 1 THEN file_size_bytes ELSE 0 END) as ps_bytes,
                        SUM(CASE WHEN is_video = 1 THEN file_size_bytes ELSE 0 END) as vs_bytes,
                        SUM(file_size_bytes) as total_bytes
                    FROM media_stats
                    GROUP BY GROUPING SETS ((), (account_email), (device_source), (album_name))
                """
                all_rows = self.execute_query(sql_consolidated, fetch_all=True)
                if not all_rows: return

                # Extract results
                totals_row = next((r for r in all_rows if r[0] == 1 and r[1] == 1 and r[2] == 1), None)
                acc_rows_raw = [r for r in all_rows if r[0] == 0 and r[3] is not None]
                dev_rows_raw = [r for r in all_rows if r[1] == 0 and r[4] is not None]
                trip_rows_raw = [r for r in all_rows if r[2] == 0 and r[5] is not None]

                if not totals_row or not totals_row[6]:
                    logger.warning("No media records found to summarize.")
                    return

                t_assets, t_photos, t_videos, ps_bytes, vs_bytes, t_bytes = totals_row[6:]
                acc_rows = [(r[3], r[6], r[7], r[8], r[9], r[10]) for r in acc_rows_raw]
                dev_rows = [(r[4], r[6], r[7], r[8], r[9], r[10]) for r in dev_rows_raw]
                trip_rows = [(r[5], r[9], r[10], r[7], r[8]) for r in trip_rows_raw]
            else:
                # Local Cache Path: Separate queries (SQLite doesn't support GROUPING SETS)
                sql_totals = """
                    SELECT 
                        COUNT(*) as total_assets,
                        SUM(CASE WHEN filename LIKE '%.jpg' OR filename LIKE '%.jpeg' OR filename LIKE '%.png' OR filename LIKE '%.gif' OR filename LIKE '%.bmp' OR filename LIKE '%.webp' OR filename LIKE '%.heic' OR filename LIKE '%.heif' OR filename LIKE '%.tiff' THEN 1 ELSE 0 END) as total_photos,
                        SUM(CASE WHEN filename LIKE '%.mp4' OR filename LIKE '%.mov' OR filename LIKE '%.avi' OR filename LIKE '%.mkv' OR filename LIKE '%.wmv' OR filename LIKE '%.flv' OR filename LIKE '%.webm' THEN 1 ELSE 0 END) as total_videos,
                        SUM(CASE WHEN filename LIKE '%.jpg' OR filename LIKE '%.jpeg' OR filename LIKE '%.png' OR filename LIKE '%.gif' OR filename LIKE '%.bmp' OR filename LIKE '%.webp' OR filename LIKE '%.heic' OR filename LIKE '%.heif' OR filename LIKE '%.tiff' THEN file_size_bytes ELSE 0 END) as photos_bytes,
                        SUM(CASE WHEN filename LIKE '%.mp4' OR filename LIKE '%.mov' OR filename LIKE '%.avi' OR filename LIKE '%.mkv' OR filename LIKE '%.wmv' OR filename LIKE '%.flv' OR filename LIKE '%.webm' THEN file_size_bytes ELSE 0 END) as videos_bytes,
                        SUM(file_size_bytes) as total_bytes
                    FROM media_library
                """
                with self._sqlite_lock:
                    self.cache_cursor.execute(sql_totals)
                    totals = self.cache_cursor.fetchone()
                
                if not totals or not totals[0]:
                    logger.warning("No media records found to summarize.")
                    return
                t_assets, t_photos, t_videos, ps_bytes, vs_bytes, t_bytes = totals

                # Get distributions from local cache
                with self._sqlite_lock:
                    # Account Distribution
                    self.cache_cursor.execute("SELECT account_email, COUNT(*), SUM(CASE WHEN filename LIKE '%.jpg' OR filename LIKE '%.jpeg' OR filename LIKE '%.png' OR filename LIKE '%.gif' OR filename LIKE '%.bmp' OR filename LIKE '%.webp' OR filename LIKE '%.heic' OR filename LIKE '%.heif' OR filename LIKE '%.tiff' THEN 1 ELSE 0 END), SUM(CASE WHEN filename LIKE '%.mp4' OR filename LIKE '%.mov' OR filename LIKE '%.avi' OR filename LIKE '%.mkv' OR filename LIKE '%.wmv' OR filename LIKE '%.flv' OR filename LIKE '%.webm' THEN 1 ELSE 0 END), SUM(CASE WHEN filename LIKE '%.jpg' OR filename LIKE '%.jpeg' OR filename LIKE '%.png' OR filename LIKE '%.gif' OR filename LIKE '%.bmp' OR filename LIKE '%.webp' OR filename LIKE '%.heic' OR filename LIKE '%.heif' OR filename LIKE '%.tiff' THEN file_size_bytes ELSE 0 END), SUM(CASE WHEN filename LIKE '%.mp4' OR filename LIKE '%.mov' OR filename LIKE '%.avi' OR filename LIKE '%.mkv' OR filename LIKE '%.wmv' OR filename LIKE '%.flv' OR filename LIKE '%.webm' THEN file_size_bytes ELSE 0 END) FROM media_library WHERE account_email IS NOT NULL GROUP BY account_email")
                    acc_rows = self.cache_cursor.fetchall()
                    # Device Distribution
                    self.cache_cursor.execute("SELECT device_source, COUNT(*), SUM(CASE WHEN filename LIKE '%.jpg' OR filename LIKE '%.jpeg' OR filename LIKE '%.png' OR filename LIKE '%.gif' OR filename LIKE '%.bmp' OR filename LIKE '%.webp' OR filename LIKE '%.heic' OR filename LIKE '%.heif' OR filename LIKE '%.tiff' THEN 1 ELSE 0 END), SUM(CASE WHEN filename LIKE '%.mp4' OR filename LIKE '%.mov' OR filename LIKE '%.avi' OR filename LIKE '%.mkv' OR filename LIKE '%.wmv' OR filename LIKE '%.flv' OR filename LIKE '%.webm' THEN 1 ELSE 0 END), SUM(CASE WHEN filename LIKE '%.jpg' OR filename LIKE '%.jpeg' OR filename LIKE '%.png' OR filename LIKE '%.gif' OR filename LIKE '%.bmp' OR filename LIKE '%.webp' OR filename LIKE '%.heic' OR filename LIKE '%.heif' OR filename LIKE '%.tiff' THEN file_size_bytes ELSE 0 END), SUM(CASE WHEN filename LIKE '%.mp4' OR filename LIKE '%.mov' OR filename LIKE '%.avi' OR filename LIKE '%.mkv' OR filename LIKE '%.wmv' OR filename LIKE '%.flv' OR filename LIKE '%.webm' THEN file_size_bytes ELSE 0 END) FROM media_library WHERE device_source IS NOT NULL GROUP BY device_source")
                    dev_rows = self.cache_cursor.fetchall()
                    # Trip Metadata
                    self.cache_cursor.execute("SELECT album_name, SUM(CASE WHEN filename LIKE '%.jpg' OR filename LIKE '%.jpeg' OR filename LIKE '%.png' OR filename LIKE '%.gif' OR filename LIKE '%.bmp' OR filename LIKE '%.webp' OR filename LIKE '%.heic' OR filename LIKE '%.heif' OR filename LIKE '%.tiff' THEN file_size_bytes ELSE 0 END), SUM(CASE WHEN filename LIKE '%.mp4' OR filename LIKE '%.mov' OR filename LIKE '%.avi' OR filename LIKE '%.mkv' OR filename LIKE '%.wmv' OR filename LIKE '%.flv' OR filename LIKE '%.webm' THEN file_size_bytes ELSE 0 END), SUM(CASE WHEN filename LIKE '%.jpg' OR filename LIKE '%.jpeg' OR filename LIKE '%.png' OR filename LIKE '%.gif' OR filename LIKE '%.bmp' OR filename LIKE '%.webp' OR filename LIKE '%.heic' OR filename LIKE '%.heif' OR filename LIKE '%.tiff' THEN 1 ELSE 0 END), SUM(CASE WHEN filename LIKE '%.mp4' OR filename LIKE '%.mov' OR filename LIKE '%.avi' OR filename LIKE '%.mkv' OR filename LIKE '%.wmv' OR filename LIKE '%.flv' OR filename LIKE '%.webm' THEN 1 ELSE 0 END) FROM media_library WHERE album_name IS NOT NULL GROUP BY album_name")
                    trip_rows = self.cache_cursor.fetchall()

            # 2. Update storage_summary (id=1)
            t_photos = t_photos or 0
            t_videos = t_videos or 0
            ps_gb = (ps_bytes or 0) / (1024**3)
            vs_gb = (vs_bytes or 0) / (1024**3)
            t_gb = (t_bytes or 0) / (1024**3)
            t_mb_all = (t_bytes or 0) / (1024**2)

            sql_upd_sum_pg = """
                INSERT INTO storage_summary (id, total_photos, total_videos, total_assets, total_photos_size_gb, total_videos_size_gb, total_size_gb)
                VALUES (1, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    total_photos = EXCLUDED.total_photos, total_videos = EXCLUDED.total_videos, total_assets = EXCLUDED.total_assets,
                    total_photos_size_gb = EXCLUDED.total_photos_size_gb, total_videos_size_gb = EXCLUDED.total_videos_size_gb,
                    total_size_gb = EXCLUDED.total_size_gb, synced_at = CURRENT_TIMESTAMP
            """
            sum_params = (t_photos, t_videos, t_assets, ps_gb, vs_gb, t_gb)
            self.execute_query(sql_upd_sum_pg, sum_params, is_write=True)
            
            if self.cache_cursor:
                with self._sqlite_lock:
                    self.cache_cursor.execute("INSERT OR REPLACE INTO storage_summary (id, total_photos, total_videos, total_assets, total_photos_size_gb, total_videos_size_gb, total_size_gb, updated_at) VALUES (1, ?, ?, ?, ?, ?, ?, (strftime('%Y-%m-%d %H:%M:%f', 'now')))", sum_params)
                    self.cache_conn.commit()

            # 3. Batch Update Account Distribution
            acc_batch = []
            for acc, total, p_count, v_count, ps_b, vs_b in acc_rows:
                ps_mb = (ps_b or 0) / (1024**2); vs_mb = (vs_b or 0) / (1024**2); t_mb = ps_mb + vs_mb
                pct = (t_mb / t_mb_all * 100) if t_mb_all > 0 else 0
                acc_batch.append((1, acc, p_count, v_count, ps_mb, vs_mb, t_mb, pct))
            
            sql_acc_batch = """
                INSERT INTO account_distribution (summary_id, account_email, photos_count, videos_count, photos_size_mb, videos_size_mb, total_size_mb, percentage)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (account_email) DO UPDATE SET
                    photos_count=EXCLUDED.photos_count, videos_count=EXCLUDED.videos_count, photos_size_mb=EXCLUDED.photos_size_mb,
                    videos_size_mb=EXCLUDED.videos_size_mb, total_size_mb=EXCLUDED.total_size_mb, percentage=EXCLUDED.percentage
            """
            self.execute_batch(sql_acc_batch, acc_batch)

            # 4. Batch Update Device Distribution
            dev_batch = []
            for dev, total, p_count, v_count, ps_b, vs_b in dev_rows:
                ps_mb = (ps_b or 0) / (1024**2); vs_mb = (vs_b or 0) / (1024**2); t_mb = ps_mb + vs_mb
                pct = (t_mb / t_mb_all * 100) if t_mb_all > 0 else 0
                dev_batch.append((1, dev, p_count, v_count, ps_mb, vs_mb, t_mb, pct))
            
            sql_dev_batch = """
                INSERT INTO device_distribution (summary_id, device_name, photos_count, videos_count, photos_size_mb, videos_size_mb, total_size_mb, percentage)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (device_name) DO UPDATE SET
                    photos_count=EXCLUDED.photos_count, videos_count=EXCLUDED.videos_count, photos_size_mb=EXCLUDED.photos_size_mb,
                    videos_size_mb=EXCLUDED.videos_size_mb, total_size_mb=EXCLUDED.total_size_mb, percentage=EXCLUDED.percentage
            """
            self.execute_batch(sql_dev_batch, dev_batch)

            # 5. Batch Update Trip Metadata
            trip_batch = []
            existing_trips = {t['name']: t.get('asset_metadata') for t in self.get_trips()}
            
            for album, p_size, v_size, p_count, v_count in trip_rows:
                new_meta = {"photos": p_size or 0, "videos": v_size or 0, "photos_count": p_count or 0, "videos_count": v_count or 0}
                new_meta_json = json.dumps(new_meta, sort_keys=True)
                
                # Compare with existing
                old_meta = existing_trips.get(album)
                if old_meta:
                    if isinstance(old_meta, str):
                        try: old_meta = json.loads(old_meta)
                        except: pass
                
                old_meta_json = json.dumps(old_meta, sort_keys=True) if old_meta else "{}"
                
                if new_meta_json != old_meta_json:
                    trip_batch.append((new_meta_json, album))
            
            if trip_batch:
                sql_trip_batch = "UPDATE trips_config SET asset_metadata = %s WHERE name = %s"
                self.execute_batch(sql_trip_batch, trip_batch)
                logger.debug(f"Applied metadata updates for {len(trip_batch)} trips.")

            logger.info("✅ High-speed batched storage summary refresh complete.")
        except Exception as e:
            logger.error(f"❌ Failed to refresh storage summary: {e}")

    def _connect_providers(self):
        if self.nhost_enabled and self.nhost_url:
            if not self._reconnect_provider('A'):
                self._handle_single_failure("Nhost (A)", "Initial connection failed")
                
        if self.neon_enabled and self.neon_url:
            if not self._reconnect_provider('B'):
                self._handle_single_failure("Neon (B)", "Initial connection failed")
                
        if not self.provider_a_active and not self.provider_b_active:
            self._handle_total_failure()

        self._migrate_cloud_schema()
            
    def _handle_single_failure(self, provider_name: str, error_msg: str):
        subject = f"Urgent: Provider {provider_name} Down"
        body = f"Provider {provider_name} failed to connect or operate.\n\nError:\n{error_msg}\n\nSwitching to Degraded Mode."
        logger.warning(subject)
        send_notification_email(subject, body)
        
    def _handle_total_failure(self):
        subject = "Critical: System Shutdown"
        last_sl_no = "Unknown"
        if self.cache_cursor:
            try:
                self.cache_cursor.execute("SELECT MAX(sl_no) FROM media_library")
                res = self.cache_cursor.fetchone()
                if res: last_sl_no = res[0]
            except: pass
                
        body = f"Both database providers are unreachable. Initiating graceful shutdown.\nLast successful sl_no: {last_sl_no}"
        logger.critical(subject + "\n" + body)
        send_notification_email(subject, body)
        sys.exit(1)

    def execute_batch(self, sql: str, params_list: list):
        """Executes a batched write operation across all active providers."""
        if not params_list: return
        
        # 1. Update Cloud Providers
        for p_id, active, conn, name in [('A', self.provider_a_active, self.conn_a, "Nhost (A)"), ('B', self.provider_b_active, self.conn_b, "Neon (B)")]:
            if active:
                try:
                    with conn.cursor() as cursor:
                        # psycopg2.extras.execute_batch is much faster for many rows
                        from psycopg2.extras import execute_batch
                        execute_batch(cursor, sql, params_list)
                        conn.commit()
                except Exception as e:
                    logger.error(f"Batch Write error on {name}: {e}")
                    if p_id == 'A': self.provider_a_active = False
                    else: self.provider_b_active = False
                    self._handle_single_failure(name, str(e))

        # 2. Update Local Cache
        if self.cache_cursor:
            sqlite_sql = sql.replace('%s', '?')
            try:
                with self._sqlite_lock:
                    self.cache_cursor.executemany(sqlite_sql, params_list)
                    self.cache_conn.commit()
            except Exception as e:
                logger.error(f"Local batch write error: {e}")

    def execute_query(self, sql: str, params=None, is_write=False, fetch_one=False, fetch_all=False):
        """Standardized query execution with try/except failover for Dual-Cloud."""
        params = params or ()
        
        if is_write:
            success_a = False
            success_b = False
            res_a = None
            res_b = None
            
            if self.provider_a_active:
                for attempt in range(2):
                    try:
                        cursor_a = self.conn_a.cursor()
                        cursor_a.execute(sql, params)
                        success_a = True
                        if fetch_one: res_a = cursor_a.fetchone()
                        elif fetch_all: res_a = cursor_a.fetchall()
                        break
                    except Exception as e:
                        if attempt == 0 and self._is_connection_error(e):
                            logger.warning(f"Provider A connection error: {e}. Attempting reconnect...")
                            if self._reconnect_provider('A'):
                                continue
                        logger.error(f"Provider A Write Failed: {e}")
                        self.provider_a_active = False
                        self._handle_single_failure("Nhost (A)", str(e))
                        break
                    
            if self.provider_b_active:
                for attempt in range(2):
                    try:
                        cursor_b = self.conn_b.cursor()
                        cursor_b.execute(sql, params)
                        success_b = True
                        if fetch_one: res_b = cursor_b.fetchone()
                        elif fetch_all: res_b = cursor_b.fetchall()
                        break
                    except Exception as e:
                        if attempt == 0 and self._is_connection_error(e):
                            logger.warning(f"Provider B connection error: {e}. Attempting reconnect...")
                            if self._reconnect_provider('B'):
                                continue
                        logger.error(f"Provider B Write Failed: {e}")
                        self.provider_b_active = False
                        self._handle_single_failure("Neon (B)", str(e))
                        break
                    
            if not self.provider_a_active and not self.provider_b_active:
                self._handle_total_failure()
                
            if (self.provider_a_active and not success_a) or (self.provider_b_active and not success_b):
                raise Exception("Synchronous mirrored write failed on an active provider!")
                
            return res_a if success_a else res_b
            
        else:
            # Round-Robin / Random Read Select
            for attempt in range(2):
                options = []
                if self.provider_a_active: options.append(('A', self.conn_a))
                if self.provider_b_active: options.append(('B', self.conn_b))
                
                if not options:
                    self._handle_total_failure()
                    
                provider_id, conn = random.choice(options)
                try:
                    cursor = conn.cursor()
                    cursor.execute(sql, params)
                    if fetch_one: return cursor.fetchone()
                    if fetch_all: return cursor.fetchall()
                    return None
                except Exception as e:
                    if attempt == 0 and self._is_connection_error(e):
                        logger.warning(f"Provider {provider_id} connection error: {e}. Attempting reconnect...")
                        if self._reconnect_provider(provider_id):
                            continue

                    logger.error(f"Provider {provider_id} Read Failed: {e}")
                    if provider_id == 'A':
                        self.provider_a_active = False
                        self._handle_single_failure("Nhost (A)", str(e))
                    else:
                        self.provider_b_active = False
                        self._handle_single_failure("Neon (B)", str(e))
                    
                    # Retry on the remaining active provider immediately
                    return self.execute_query(sql, params, is_write=False, fetch_one=fetch_one, fetch_all=fetch_all)

    def _sync_sequences(self):
        """Ensures the auto-increment sequences are up to date with the max sl_no or id."""
        for active, conn, name in [(self.provider_a_active, self.conn_a, "Nhost (A)"), 
                                   (self.provider_b_active, self.conn_b, "Neon (B)")]:
            if active:
                try:
                    cursor = conn.cursor()
                    
                    # Sync media_library sequence
                    cursor.execute("SELECT COALESCE(MAX(sl_no), 1) FROM media_library")
                    max_val_media = cursor.fetchone()[0]
                    cursor.execute("SELECT pg_get_serial_sequence('media_library', 'sl_no')")
                    seq_res_media = cursor.fetchone()
                    seq_name_media = seq_res_media[0] if seq_res_media and seq_res_media[0] else 'media_library_sl_no_seq'
                    cursor.execute("SELECT setval(%s, %s)", (seq_name_media, max_val_media))
                    
                    # Sync trips_config sequence
                    cursor.execute("SELECT COALESCE(MAX(sl_no), 1) FROM trips_config")
                    max_val_trips = cursor.fetchone()[0]
                    cursor.execute("SELECT pg_get_serial_sequence('trips_config', 'sl_no')")
                    seq_res_trips = cursor.fetchone()
                    seq_name_trips = seq_res_trips[0] if seq_res_trips and seq_res_trips[0] else 'trips_config_sl_no_seq'
                    cursor.execute("SELECT setval(%s, %s)", (seq_name_trips, max_val_trips))
                    
                    # Sync summary tables sequences
                    for table in ["storage_summary", "account_distribution", "device_distribution"]:
                        cursor.execute(f"SELECT COALESCE(MAX(id), 1) FROM {table}")
                        max_val = cursor.fetchone()[0]
                        cursor.execute(f"SELECT pg_get_serial_sequence('{table}', 'id')")
                        seq_res = cursor.fetchone()
                        seq_name = seq_res[0] if seq_res and seq_res[0] else f'{table}_id_seq'
                        cursor.execute("SELECT setval(%s, %s)", (seq_name, max_val))
                    
                    logger.debug(f"Synced sequences on {name}")
                except Exception as e:
                    logger.warning(f"⚠️ Could not sync sequences on {name}: {e}")

    def _reconcile_table_timestamp(self, table_name: str, last_sync_time: str):
        """Reconciles a table using updated_at and last_sync_time across all providers (A, B, and Local)."""
        all_changes = {} # file_hash/pk -> latest_row

        cols_map = {
            "media_library": ['sl_no', 'file_hash', 'filename', 'file_size_bytes', 'upload_date', 'account_email', 'device_source', 'remote_id', 'album_name', 'updated_at'],
            "trips_config": ['sl_no', 'name', 'start', 'end', 'require_gps', 'album_id', 'album_url', 'email_message_id', 'asset_metadata', 'updated_at'],
            "device_config": ['device_name', 'directories', 'sl_no', 'updated_at'],
            "storage_summary": ['id', 'synced_at', 'total_photos', 'total_videos', 'total_assets', 'total_photos_size_gb', 'total_videos_size_gb', 'total_size_gb', 'updated_at'],
            "account_distribution": ['id', 'summary_id', 'account_email', 'photos_count', 'videos_count', 'photos_size_mb', 'videos_size_mb', 'total_size_mb', 'percentage', 'updated_at'],
            "device_distribution": ['id', 'summary_id', 'device_name', 'photos_count', 'videos_count', 'photos_size_mb', 'videos_size_mb', 'total_size_mb', 'percentage', 'updated_at']
        }
        pk_map = {
            "media_library": "file_hash",
            "trips_config": "name",
            "device_config": "device_name",
            "storage_summary": "id",
            "account_distribution": "account_email",
            "device_distribution": "device_name"
        }

        if table_name not in cols_map: return

        cols = cols_map[table_name]
        pk = pk_map[table_name]
        # Quote col names for safety
        safe_cols = [f'"{c}"' if c in ["end", "order"] else f'"{c}"' for c in cols]
        cols_str = ", ".join(safe_cols)

        # 1. Collect changes from ALL sources (Cloud A, Cloud B, Local Cache)
        sources = []
        if self.provider_a_active: sources.append((self.conn_a, "Nhost (A)", "postgres"))
        if self.provider_b_active: sources.append((self.conn_b, "Neon (B)", "postgres"))
        if self.cache_conn: sources.append((self.cache_conn, "Local Cache", "sqlite"))

        max_ts_seen = None

        for conn, name, db_type in sources:
            try:
                if db_type == "postgres":
                    cursor = conn.cursor()
                    cursor.execute(f'SELECT {cols_str} FROM "{table_name}" WHERE updated_at > %s', (last_sync_time,))
                    rows = cursor.fetchall()
                else:
                    with self._sqlite_lock:
                        cursor = conn.cursor()
                        cursor.execute(f'SELECT {cols_str} FROM "{table_name}" WHERE updated_at > ?', (last_sync_time,))
                        rows = cursor.fetchall()
                
                if rows:
                    for row in rows:
                        row_dict = dict(zip(cols, row))
                        key = row_dict[pk]
                        
                        # Track the latest timestamp we've encountered anywhere
                        ts_str = str(row_dict['updated_at'])
                        if not max_ts_seen or ts_str > max_ts_seen:
                            max_ts_seen = ts_str

                        # --- Special Clause for trips_config ---
                        # Skip if ONLY asset_metadata (and updated_at) changed, as it's recalculated locally.
                        if table_name == "trips_config":
                            existing = None
                            if self.cache_cursor:
                                with self._sqlite_lock:
                                    self.cache_cursor.execute(f'SELECT {cols_str} FROM "{table_name}" WHERE "{pk}" = ?', (key,))
                                    e_row = self.cache_cursor.fetchone()
                                    if e_row: existing = dict(zip(cols, e_row))
                            
                            if existing:
                                # Compare all columns EXCEPT asset_metadata and updated_at
                                significant_change = False
                                for c in cols:
                                    if c in ["asset_metadata", "updated_at", "sl_no"]: continue
                                    
                                    val_a = row_dict.get(c)
                                    val_b = existing.get(c)

                                    # Normalize for comparison
                                    # 1. Booleans (PG might return True/False, SQLite 1/0)
                                    if isinstance(val_a, bool) or c == "require_gps":
                                        val_a = 1 if val_a and str(val_a).lower() not in ("false", "0", "none") else 0
                                        val_b = 1 if val_b and str(val_b).lower() not in ("false", "0", "none") else 0
                                    
                                    # 2. Dates/Strings
                                    if val_a is None or str(val_a).lower() == "none": val_a = ""
                                    if val_b is None or str(val_b).lower() == "none": val_b = ""
                                    
                                    if str(val_a).strip() != str(val_b).strip():
                                        significant_change = True
                                        break
                                if not significant_change:
                                    continue # Skip this row

                        # Track the latest version found across all sources
                        if key not in all_changes or ts_str > str(all_changes[key]['updated_at']):
                            all_changes[key] = row_dict
            except Exception as e:
                logger.error(f"Failed to fetch incremental changes from {name} for {table_name}: {e}")

        if not all_changes: 
            return max_ts_seen

        logger.info(f"📥 Found {len(all_changes)} significant changes for {table_name}")
        # 2. Apply latest changes in batches to all providers
        batch_size = 500
        change_items = list(all_changes.values())
        
        update_cols = [c for c in cols if c != pk]
        # PG specific: only update if the incoming updated_at is actually newer than what we have
        update_str = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in update_cols])
        
        for i in range(0, len(change_items), batch_size):
            batch = change_items[i:i + batch_size]
            
            # Prepare processed batch parameters (converting dicts to JSON strings)
            # This ensures compatibility across all DB types (Postgres JSONB and SQLite TEXT)
            processed_batch = []
            for row_dict in batch:
                row_vals = []
                for c in cols:
                    val = row_dict[c]
                    if isinstance(val, dict):
                        val = json.dumps(val)
                    row_vals.append(val)
                processed_batch.append(tuple(row_vals))

            # --- PostgreSQL Batched UPSERT ---
            # Single-row upsert template for execute_batch
            upsert_template = f'INSERT INTO "{table_name}" ({cols_str}) VALUES ({", ".join(["%s"] * len(cols))}) ON CONFLICT ("{pk}") DO UPDATE SET {update_str} WHERE EXCLUDED.updated_at > "{table_name}".updated_at'
            
            from psycopg2.extras import execute_batch

            for active, conn, name in [(self.provider_a_active, self.conn_a, "Nhost (A)"), (self.provider_b_active, self.conn_b, "Neon (B)")]:
                if active:
                    try:
                        with conn.cursor() as cursor:
                            execute_batch(cursor, upsert_template, processed_batch)
                            conn.commit()
                    except Exception as e:
                        logger.error(f"Failed to batch sync to {name} for {table_name}: {e}")

            # --- SQLite Batched UPSERT ---
            if self.cache_cursor:
                sqlite_placeholders = ", ".join(["?" for _ in cols])
                sqlite_upsert = f'INSERT OR REPLACE INTO "{table_name}" ({cols_str}) VALUES ({sqlite_placeholders})'
                try:
                    with self._sqlite_lock:
                        self.cache_cursor.executemany(sqlite_upsert, processed_batch)
                        self.cache_conn.commit()
                except Exception as e:
                    logger.error(f"Failed to batch sync to local cache for {table_name}: {e}")

        logger.info(f"✅ Incrementally synced {len(change_items)} records for {table_name}")
        # Always return the latest timestamp seen (from changes or from max_ts_seen)
        latest_ts = None
        if all_changes:
            latest_ts = max(str(row['updated_at']) for row in all_changes.values())
        if max_ts_seen and (not latest_ts or max_ts_seen > latest_ts):
            latest_ts = max_ts_seen
            
        return latest_ts

    def reconcile_databases(self):
        """Self-Heal Phase: Reconciles databases using timestamp-based sync."""
        logger.info(f"🔍 [v1.1] Running Initialization & Timestamp-based Reconciliation...")
        
        # Get last sync time
        last_sync_time = '1970-01-01 00:00:00'
        if self.cache_cursor:
            with self._sqlite_lock:
                self.cache_cursor.execute("SELECT last_sync_time FROM sync_tracker WHERE id = 1")
                res = self.cache_cursor.fetchone()
                if res: last_sync_time = res[0]
        else:
            res = self.execute_query("SELECT last_sync_time FROM sync_tracker WHERE id = 1", fetch_one=True)
            if res: last_sync_time = res[0]

        # Sync all tables EXCEPT summary tables (which are recalculated)
        sync_results = []
        for table in ["media_library", "trips_config", "device_config"]:
            res = self._reconcile_table_timestamp(table, last_sync_time)
            if res: sync_results.append(res)

        # Startup check: If any trip has null metadata, force a summary refresh
        needs_refresh = False
        all_trips = self.get_trips()
        if not all_trips:
            needs_refresh = True
        else:
            for t in all_trips:
                if not t.get('asset_metadata'):
                    needs_refresh = True
                    break
        
        if needs_refresh:
            logger.info("Trip metadata missing or incomplete. Triggering full refresh...")
            self.refresh_storage_summary(use_local_for_calc=True)

        if sync_results:
            new_last_sync = max(sync_results)
            sql_upd = "UPDATE sync_tracker SET last_sync_time = %s WHERE id = 1"
            self.execute_query(sql_upd, (new_last_sync,), is_write=True)
            if self.cache_cursor:
                with self._sqlite_lock:
                    self.cache_cursor.execute("UPDATE sync_tracker SET last_sync_time = ? WHERE id = 1", (new_last_sync,))
                    self.cache_conn.commit()
            logger.info(f"✅ Sync complete. New last_sync_time: {new_last_sync}")
            
        self._sync_sequences()

    def init_local_cache(self):
        """Initializes transient local SQLite connection (thread-safe)."""
        try:
            cache_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "Data", "local_cache.db")
            os.makedirs(os.path.dirname(cache_path), exist_ok=True)
            # check_same_thread=False allows the connection to be shared across worker threads.
            # All access is serialised by self._sqlite_lock.
            self.cache_conn = sqlite3.connect(cache_path, check_same_thread=False)
            self.cache_cursor = self.cache_conn.cursor()
            
            # Ensure table schemas exist in SQLite
            self.cache_conn.executescript('''
                CREATE TABLE IF NOT EXISTS media_library (
                    file_hash TEXT PRIMARY KEY,
                    sl_no INTEGER,
                    filename TEXT,
                    file_size_bytes INTEGER,
                    upload_date TEXT,
                    account_email TEXT,
                    device_source TEXT,
                    remote_id TEXT,
                    album_name TEXT,
                    updated_at TIMESTAMP DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now'))
                );

                CREATE TABLE IF NOT EXISTS trips_config (
                    name TEXT PRIMARY KEY,
                    sl_no INTEGER,
                    start TEXT,
                    "end" TEXT,
                    require_gps BOOLEAN,
                    album_id TEXT,
                    album_url TEXT,
                    email_message_id TEXT,
                    asset_metadata TEXT,
                    updated_at TIMESTAMP DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now'))
                );

                CREATE TABLE IF NOT EXISTS device_config (
                    device_name TEXT PRIMARY KEY,
                    directories TEXT,
                    sl_no INTEGER,
                    updated_at TIMESTAMP DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now'))
                );

                CREATE TABLE IF NOT EXISTS storage_summary (
                    id INTEGER PRIMARY KEY,
                    synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    total_photos INTEGER DEFAULT 0,
                    total_videos INTEGER DEFAULT 0,
                    total_assets INTEGER DEFAULT 0,
                    total_photos_size_gb REAL DEFAULT 0,
                    total_videos_size_gb REAL DEFAULT 0,
                    total_size_gb REAL DEFAULT 0,
                    updated_at TIMESTAMP DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now'))
                );

                CREATE TABLE IF NOT EXISTS account_distribution (
                    account_email TEXT PRIMARY KEY,
                    id INTEGER,
                    summary_id INTEGER REFERENCES storage_summary(id) ON DELETE CASCADE,
                    photos_count INTEGER DEFAULT 0,
                    videos_count INTEGER DEFAULT 0,
                    photos_size_mb REAL DEFAULT 0,
                    videos_size_mb REAL DEFAULT 0,
                    total_size_mb REAL DEFAULT 0,
                    percentage REAL DEFAULT 0,
                    updated_at TIMESTAMP DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now'))
                );

                CREATE TABLE IF NOT EXISTS device_distribution (
                    device_name TEXT PRIMARY KEY,
                    id INTEGER,
                    summary_id INTEGER REFERENCES storage_summary(id) ON DELETE CASCADE,
                    photos_count INTEGER DEFAULT 0,
                    videos_count INTEGER DEFAULT 0,
                    photos_size_mb REAL DEFAULT 0,
                    videos_size_mb REAL DEFAULT 0,
                    total_size_mb REAL DEFAULT 0,
                    percentage REAL DEFAULT 0,
                    updated_at TIMESTAMP DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now'))
                );

                CREATE TABLE IF NOT EXISTS sync_tracker (
                    id INTEGER PRIMARY KEY,
                    last_sync_time TIMESTAMP DEFAULT '1970-01-01 00:00:00'
                );
                INSERT OR IGNORE INTO sync_tracker (id, last_sync_time) VALUES (1, '1970-01-01 00:00:00');
            ''')

            # Add triggers for all tables
            for table, pk in [
                ("media_library", "file_hash"),
                ("trips_config", "name"),
                ("device_config", "device_name"),
                ("storage_summary", "id"),
                ("account_distribution", "account_email"),
                ("device_distribution", "device_name")
            ]:
                self.cache_conn.execute(f'''
                    CREATE TRIGGER IF NOT EXISTS update_{table}_timestamp 
                    AFTER UPDATE ON {table}
                    FOR EACH ROW
                    WHEN (NEW.updated_at IS OLD.updated_at)
                    BEGIN
                        UPDATE {table} SET updated_at = (strftime('%Y-%m-%d %H:%M:%f', 'now')) WHERE "{pk}" = OLD."{pk}";
                    END;
                ''')

            self.cache_conn.execute("CREATE INDEX IF NOT EXISTS idx_filename ON media_library(filename)")
            self.cache_conn.execute("CREATE INDEX IF NOT EXISTS idx_filename_nocase ON media_library(filename COLLATE NOCASE)")
            self.cache_conn.execute("CREATE INDEX IF NOT EXISTS idx_album ON media_library(album_name)")
            self.cache_conn.execute("CREATE INDEX IF NOT EXISTS idx_remote ON media_library(remote_id)")
            self.cache_conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_hash ON media_library(file_hash)")
            
            self.cache_conn.commit()
            
            # Migration: add columns to existing tables if they don't exist
            migration_queries = [
                ("media_library", "updated_at", "ALTER TABLE media_library ADD COLUMN updated_at TIMESTAMP DEFAULT '1970-01-01 00:00:00'"),
                ("trips_config", "updated_at", "ALTER TABLE trips_config ADD COLUMN updated_at TIMESTAMP DEFAULT '1970-01-01 00:00:00'"),
                ("device_config", "updated_at", "ALTER TABLE device_config ADD COLUMN updated_at TIMESTAMP DEFAULT '1970-01-01 00:00:00'"),
                ("storage_summary", "updated_at", "ALTER TABLE storage_summary ADD COLUMN updated_at TIMESTAMP DEFAULT '1970-01-01 00:00:00'"),
                ("account_distribution", "updated_at", "ALTER TABLE account_distribution ADD COLUMN updated_at TIMESTAMP DEFAULT '1970-01-01 00:00:00'"),
                ("device_distribution", "updated_at", "ALTER TABLE device_distribution ADD COLUMN updated_at TIMESTAMP DEFAULT '1970-01-01 00:00:00'"),
                ("device_config", "sl_no", "ALTER TABLE device_config ADD COLUMN sl_no INTEGER"),
                ("trips_config", "album_url", "ALTER TABLE trips_config ADD COLUMN album_url TEXT"),
                ("trips_config", "email_message_id", "ALTER TABLE trips_config ADD COLUMN email_message_id TEXT"),
                ("trips_config", "asset_metadata", "ALTER TABLE trips_config ADD COLUMN asset_metadata TEXT"),
                ("storage_summary", "total_photos_size_gb", "ALTER TABLE storage_summary ADD COLUMN total_photos_size_gb REAL DEFAULT 0"),
                ("storage_summary", "total_videos_size_gb", "ALTER TABLE storage_summary ADD COLUMN total_videos_size_gb REAL DEFAULT 0"),
                ("account_distribution", "photos_size_mb", "ALTER TABLE account_distribution ADD COLUMN photos_size_mb REAL DEFAULT 0"),
                ("account_distribution", "videos_size_mb", "ALTER TABLE account_distribution ADD COLUMN videos_size_mb REAL DEFAULT 0"),
                ("account_distribution", "total_size_mb", "ALTER TABLE account_distribution ADD COLUMN total_size_mb REAL DEFAULT 0"),
                ("account_distribution", "percentage", "ALTER TABLE account_distribution ADD COLUMN percentage REAL DEFAULT 0"),
                ("device_distribution", "photos_size_mb", "ALTER TABLE device_distribution ADD COLUMN photos_size_mb REAL DEFAULT 0"),
                ("device_distribution", "videos_size_mb", "ALTER TABLE device_distribution ADD COLUMN videos_size_mb REAL DEFAULT 0"),
                ("device_distribution", "total_size_mb", "ALTER TABLE device_distribution ADD COLUMN total_size_mb REAL DEFAULT 0"),
                ("device_distribution", "percentage", "ALTER TABLE device_distribution ADD COLUMN percentage REAL DEFAULT 0")
            ]

            for table, col, query in migration_queries:
                try:
                    # Check if column exists using PRAGMA table_info
                    cursor = self.cache_conn.cursor()
                    cursor.execute(f"PRAGMA table_info({table})")
                    columns = [c[1] for c in cursor.fetchall()]
                    if col not in columns:
                        logger.info(f"🚀 Migrating {table}: Adding column {col}...")
                        # Run ALTER TABLE on connection directly
                        self.cache_conn.execute(query)
                        self.cache_conn.commit()
                        logger.info(f"✅ Successfully added {col} to {table}")
                except sqlite3.OperationalError as e:
                    logger.warning(f"⚠️ OperationalError during migration for {table}.{col}: {e}")
                except Exception as e:
                    logger.error(f"❌ Unexpected migration failure for {table}.{col}: {e}")

            # Final cursor refresh
            self.cache_cursor = self.cache_conn.cursor()
            logger.info(f"✅ Local Cache initialized at {cache_path}")
            
        except Exception as e:
            logger.error(f"❌ Failed to init local cache: {e}")

    def sync_cloud_to_local(self):
        """No-op: Legacy sync disabled in favor of new timestamp-based reconciliation."""
        pass

    def get_all_media_filenames(self):
        """Returns a set of all filenames in media_library (lowercase) for priming the filename cache. Uses local cache when available."""
        if not self.cache_cursor:
            return set()
        try:
            with self._sqlite_lock:
                self.cache_cursor.execute("SELECT filename FROM media_library")
                rows = self.cache_cursor.fetchall()
            return {r[0].lower() for r in rows if r and r[0]}
        except Exception as e:
            logger.warning(f"Could not load filenames from DB for cache priming: {e}")
            return set()

    def get_album_item_count(self, album_name: str, account_email: str) -> int:
        """Returns the number of media items associated with a specific album and account."""
        sql = "SELECT COUNT(*) FROM media_library WHERE album_name = %s AND account_email = %s AND remote_id IS NOT NULL"
        if self.cache_cursor:
            try:
                with self._sqlite_lock:
                    self.cache_cursor.execute("SELECT COUNT(*) FROM media_library WHERE album_name = ? AND account_email = ? AND remote_id IS NOT NULL", (album_name, account_email))
                    res = self.cache_cursor.fetchone()
                    return res[0] if res else 0
            except Exception as e:
                logger.error(f"Local cache query for album count failed: {e}")
        
        res = self.execute_query(sql, (album_name, account_email), fetch_one=True)
        return res[0] if res else 0

    def get_album_remote_ids(self, album_name: str, account_email: str) -> set:
        """Fetches all remote_ids associated with a specific album name and account email from the local cache (fallback to cloud)."""
        sql = "SELECT remote_id FROM media_library WHERE album_name = %s AND account_email = %s AND remote_id IS NOT NULL"
        
        if self.cache_cursor:
            try:
                with self._sqlite_lock:
                    self.cache_cursor.execute("SELECT remote_id FROM media_library WHERE album_name = ? AND account_email = ? AND remote_id IS NOT NULL", (album_name, account_email))
                    rows = self.cache_cursor.fetchall()
                    return {r[0] for r in rows if r and r[0]}
            except Exception as e:
                logger.error(f"Local cache query for album remote IDs failed: {e}")
                
        rows = self.execute_query(sql, (album_name, account_email), fetch_all=True)
        if rows:
            return {r[0] for r in rows if r and r[0]}
        return set()

    def adopt_photos_to_album_batch(self, remote_ids: list, album_name: str):
        """Links a list of existing photos in the DB to an album and updates trip metadata in one batch."""
        if not remote_ids:
            return
            
        try:
            logger.info(f"🔗 Batched adoption of {len(remote_ids)} items into album '{album_name}'...")
            
            # 1. Fetch file data for all IDs in one go to calculate metadata changes
            file_data_list = []
            placeholders = ", ".join(["%s"] * len(remote_ids))
            sql_get = f"SELECT file_size_bytes, filename FROM media_library WHERE remote_id IN ({placeholders})"
            
            # Use local cache if possible for faster read
            if self.cache_cursor:
                with self._sqlite_lock:
                    sqlite_placeholders = ", ".join(["?"] * len(remote_ids))
                    self.cache_cursor.execute(f"SELECT file_size_bytes, filename FROM media_library WHERE remote_id IN ({sqlite_placeholders})", tuple(remote_ids))
                    rows = self.cache_cursor.fetchall()
                    file_data_list = [{"size": r[0], "name": r[1]} for r in rows if r]
            else:
                rows = self.execute_query(sql_get, tuple(remote_ids), fetch_all=True)
                if rows:
                    file_data_list = [{"size": r[0], "name": r[1]} for r in rows if r]

            # 2. Batch Update media_library (Cloud + Local)
            # We update album_name only if it's currently different or NULL
            sql_upd = f'UPDATE media_library SET album_name = %s, updated_at = CURRENT_TIMESTAMP WHERE remote_id IN ({placeholders}) AND (album_name IS NULL OR album_name != %s)'
            self.execute_query(sql_upd, tuple([album_name] + remote_ids + [album_name]), is_write=True)
            
            if self.cache_cursor:
                with self._sqlite_lock:
                    sqlite_placeholders = ", ".join(["?"] * len(remote_ids))
                    self.cache_cursor.execute(f"UPDATE media_library SET album_name = ?, updated_at = (strftime('%Y-%m-%d %H:%M:%f', 'now')) WHERE remote_id IN ({sqlite_placeholders}) AND (album_name IS NULL OR album_name != ?)", tuple([album_name] + remote_ids + [album_name]))
                    self.cache_conn.commit()

            # 3. Calculate Aggregated Metadata Changes
            total_p_size = 0
            total_p_count = 0
            total_v_size = 0
            total_v_count = 0
            
            photo_exts = {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp', '.heic', '.heif', '.tiff'}
            video_exts = {'.mp4', '.mov', '.avi', '.mkv', '.wmv', '.flv', '.webm'}
            
            for item in file_data_list:
                size = item.get("size") or 0
                name = item.get("name") or ""
                ext = os.path.splitext(name.lower())[1]
                if ext in photo_exts:
                    total_p_size += size
                    total_p_count += 1
                elif ext in video_exts:
                    total_v_size += size
                    total_v_count += 1

            # 4. Single update to trips_config
            if total_p_count > 0 or total_v_count > 0:
                curr_meta = {}
                sql_meta = "SELECT asset_metadata FROM trips_config WHERE name = %s"
                if self.cache_cursor:
                    with self._sqlite_lock:
                        self.cache_cursor.execute("SELECT asset_metadata FROM trips_config WHERE name = ?", (album_name,))
                        res = self.cache_cursor.fetchone()
                        if res and res[0]: curr_meta = json.loads(res[0])
                else:
                    res = self.execute_query(sql_meta, (album_name,), fetch_one=True)
                    if res and res[0]: curr_meta = res[0] if isinstance(res[0], dict) else json.loads(res[0])

                # Increment metadata
                if 'photos' not in curr_meta: curr_meta['photos'] = 0
                if 'photos_count' not in curr_meta: curr_meta['photos_count'] = 0
                if 'videos' not in curr_meta: curr_meta['videos'] = 0
                if 'videos_count' not in curr_meta: curr_meta['videos_count'] = 0

                curr_meta['photos'] += total_p_size
                curr_meta['photos_count'] += total_p_count
                curr_meta['videos'] += total_v_size
                curr_meta['videos_count'] += total_v_count
                
                new_meta_json = json.dumps(curr_meta)
                sql_meta_upd = "UPDATE trips_config SET asset_metadata = %s WHERE name = %s"
                self.execute_query(sql_meta_upd, (new_meta_json, album_name), is_write=True)
                if self.cache_cursor:
                    with self._sqlite_lock:
                        self.cache_cursor.execute("UPDATE trips_config SET asset_metadata = ? WHERE name = ?", (new_meta_json, album_name))
                        self.cache_conn.commit()
            
            logger.info(f"✅ Successfully batched adoption for {len(remote_ids)} items.")
        except Exception as e:
            logger.error(f"❌ Failed batched adoption: {e}")

    def remove_photos_from_album_batch(self, remote_ids: list, album_name: str):
        """Removes the album association for a list of photos in both cloud and local cache and updates trip metadata in one batch."""
        if not remote_ids:
            return
            
        try:
            logger.info(f"🗑️ Batched removal of {len(remote_ids)} items from album '{album_name}'...")
            
            # 1. Fetch file data for all IDs in one go to calculate metadata changes
            file_data_list = []
            placeholders = ", ".join(["%s"] * len(remote_ids))
            sql_get = f"SELECT file_size_bytes, filename FROM media_library WHERE remote_id IN ({placeholders})"
            
            # Use local cache if possible for faster read
            if self.cache_cursor:
                with self._sqlite_lock:
                    sqlite_placeholders = ", ".join(["?"] * len(remote_ids))
                    self.cache_cursor.execute(f"SELECT file_size_bytes, filename FROM media_library WHERE remote_id IN ({sqlite_placeholders})", tuple(remote_ids))
                    rows = self.cache_cursor.fetchall()
                    file_data_list = [{"size": r[0], "name": r[1]} for r in rows if r]
            else:
                rows = self.execute_query(sql_get, tuple(remote_ids), fetch_all=True)
                if rows:
                    file_data_list = [{"size": r[0], "name": r[1]} for r in rows if r]

            # 2. Batch Update media_library (Cloud + Local)
            sql_upd = f'UPDATE media_library SET album_name = NULL, updated_at = CURRENT_TIMESTAMP WHERE remote_id IN ({placeholders}) AND album_name = %s'
            self.execute_query(sql_upd, tuple(remote_ids + [album_name]), is_write=True)
            
            if self.cache_cursor:
                with self._sqlite_lock:
                    sqlite_placeholders = ", ".join(["?"] * len(remote_ids))
                    self.cache_cursor.execute(f"UPDATE media_library SET album_name = NULL, updated_at = (strftime('%Y-%m-%d %H:%M:%f', 'now')) WHERE remote_id IN ({sqlite_placeholders}) AND album_name = ?", tuple(remote_ids + [album_name]))
                    self.cache_conn.commit()

            # 3. Calculate Aggregated Metadata Changes
            total_p_size = 0
            total_p_count = 0
            total_v_size = 0
            total_v_count = 0
            
            photo_exts = {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp', '.heic', '.heif', '.tiff'}
            video_exts = {'.mp4', '.mov', '.avi', '.mkv', '.wmv', '.flv', '.webm'}
            
            for item in file_data_list:
                size = item.get("size") or 0
                name = item.get("name") or ""
                ext = os.path.splitext(name.lower())[1]
                if ext in photo_exts:
                    total_p_size += size
                    total_p_count += 1
                elif ext in video_exts:
                    total_v_size += size
                    total_v_count += 1

            # 4. Single update to trips_config
            if total_p_count > 0 or total_v_count > 0:
                curr_meta = {}
                sql_meta = "SELECT asset_metadata FROM trips_config WHERE name = %s"
                if self.cache_cursor:
                    with self._sqlite_lock:
                        self.cache_cursor.execute("SELECT asset_metadata FROM trips_config WHERE name = ?", (album_name,))
                        res = self.cache_cursor.fetchone()
                        if res and res[0]: curr_meta = json.loads(res[0])
                else:
                    res = self.execute_query(sql_meta, (album_name,), fetch_one=True)
                    if res and res[0]: curr_meta = res[0] if isinstance(res[0], dict) else json.loads(res[0])

                if curr_meta:
                    curr_meta['photos'] = max(0, curr_meta.get('photos', 0) - total_p_size)
                    curr_meta['photos_count'] = max(0, curr_meta.get('photos_count', 0) - total_p_count)
                    curr_meta['videos'] = max(0, curr_meta.get('videos', 0) - total_v_size)
                    curr_meta['videos_count'] = max(0, curr_meta.get('videos_count', 0) - total_v_count)
                    
                    new_meta_json = json.dumps(curr_meta)
                    sql_meta_upd = "UPDATE trips_config SET asset_metadata = %s WHERE name = %s"
                    self.execute_query(sql_meta_upd, (new_meta_json, album_name), is_write=True)
                    if self.cache_cursor:
                        with self._sqlite_lock:
                            self.cache_cursor.execute("UPDATE trips_config SET asset_metadata = ? WHERE name = ?", (new_meta_json, album_name))
                            self.cache_conn.commit()
            
            logger.info(f"✅ Successfully batched removal for {len(remote_ids)} items.")
        except Exception as e:
            logger.error(f"❌ Failed batched removal: {e}")

    def remove_photo_from_album_record(self, remote_id: str, album_name: str):
        """Removes the album association for a specific photo in both cloud and local cache and updates trip metadata."""
        try:
            # First, fetch file data to know what sizes to subtract
            file_data = None
            sql_get = "SELECT file_size_bytes, filename FROM media_library WHERE remote_id = %s LIMIT 1"
            if self.cache_cursor:
                with self._sqlite_lock:
                    self.cache_cursor.execute("SELECT file_size_bytes, filename FROM media_library WHERE remote_id = ? LIMIT 1", (remote_id,))
                    res = self.cache_cursor.fetchone()
                    if res: file_data = {"file_size_bytes": res[0], "filename": res[1]}
            else:
                res = self.execute_query(sql_get, (remote_id,), fetch_one=True)
                if res: file_data = {"file_size_bytes": res[0], "filename": res[1]}

            sql_upd = "UPDATE media_library SET album_name = NULL WHERE remote_id = %s AND album_name = %s"
            self.execute_query(sql_upd, (remote_id, album_name), is_write=True)
            
            if self.cache_cursor:
                with self._sqlite_lock:
                    self.cache_cursor.execute("UPDATE media_library SET album_name = NULL WHERE remote_id = ? AND album_name = ?", (remote_id, album_name))
                    self.cache_conn.commit()
                logger.debug(f"🗑️ Removed album association for {remote_id} from cache & cloud.")

            # Incremental decrement of trip metadata
            if file_data and album_name:
                size_bytes = file_data.get("file_size_bytes", 0)
                filename = file_data.get("filename", "")
                
                photo_exts = {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp', '.heic', '.heif', '.tiff'}
                video_exts = {'.mp4', '.mov', '.avi', '.mkv', '.wmv', '.flv', '.webm'}
                ext = os.path.splitext(filename or "")[1].lower()
                is_photo = ext in photo_exts
                is_video = ext in video_exts

                if is_photo or is_video:
                    curr_meta = {}
                    sql_meta = "SELECT asset_metadata FROM trips_config WHERE name = %s"
                    if self.cache_cursor:
                        with self._sqlite_lock:
                            self.cache_cursor.execute("SELECT asset_metadata FROM trips_config WHERE name = ?", (album_name,))
                            res = self.cache_cursor.fetchone()
                            if res and res[0]: curr_meta = json.loads(res[0])
                    else:
                        res = self.execute_query(sql_meta, (album_name,), fetch_one=True)
                        if res and res[0]: curr_meta = res[0] if isinstance(res[0], dict) else json.loads(res[0])

                    if curr_meta:
                        if is_photo:
                            curr_meta['photos'] = max(0, curr_meta.get('photos', 0) - size_bytes)
                            curr_meta['photos_count'] = max(0, curr_meta.get('photos_count', 0) - 1)
                        elif is_video:
                            curr_meta['videos'] = max(0, curr_meta.get('videos', 0) - size_bytes)
                            curr_meta['videos_count'] = max(0, curr_meta.get('videos_count', 0) - 1)
                        
                        new_meta_json = json.dumps(curr_meta)
                        sql_meta_upd = "UPDATE trips_config SET asset_metadata = %s WHERE name = %s"
                        self.execute_query(sql_meta_upd, (new_meta_json, album_name), is_write=True)
                        if self.cache_cursor:
                            with self._sqlite_lock:
                                self.cache_cursor.execute("UPDATE trips_config SET asset_metadata = ? WHERE name = ?", (new_meta_json, album_name))
                                self.cache_conn.commit()
        except Exception as e:
            logger.error(f"❌ Failed to remove photo from album record and update metadata: {e}")

    def file_exists_by_name(self, filename: str) -> bool:
        """Phase 1: Local Cache Check."""
        filenames_to_check = [filename]
        
        lower_name = filename.lower()
        if lower_name.endswith('.jpg'):
            filenames_to_check.extend([filename[:-4] + '.jpeg', filename[:-4] + '.JPEG'])
        elif lower_name.endswith('.jpeg'):
            filenames_to_check.extend([filename[:-5] + '.jpg', filename[:-5] + '.JPG'])

        if self.cache_cursor:
            try:
                with self._sqlite_lock:
                    for fname in filenames_to_check:
                        self.cache_cursor.execute("SELECT 1 FROM media_library WHERE filename = ? COLLATE NOCASE LIMIT 1", (fname,))
                        if self.cache_cursor.fetchone() is not None:
                            return True
                return False
            except Exception as e:
                logger.error(f"Local cache query failed: {e}")
                
        for fname in filenames_to_check:
            sql = "SELECT 1 FROM media_library WHERE filename ILIKE %s LIMIT 1"
            res = self.execute_query(sql, (fname,), fetch_one=True)
            if res: return True
        return False

    def file_exists_by_hash(self, file_hash: str) -> bool:
        """Phase 2: Local Cache Check."""
        if self.cache_cursor:
            try:
                with self._sqlite_lock:
                    self.cache_cursor.execute("SELECT 1 FROM media_library WHERE file_hash = ? LIMIT 1", (file_hash,))
                    if self.cache_cursor.fetchone() is not None:
                        return True
                return False
            except Exception as e:
                logger.error(f"Local cache query failed: {e}")
                
        sql = "SELECT 1 FROM media_library WHERE file_hash = %s LIMIT 1"
        res = self.execute_query(sql, (file_hash,), fetch_one=True)
        if res: return True
        return False

    def get_file_by_hash(self, file_hash: str) -> dict:
        """Phase 2: Local Cache Check, returns record dict if found."""
        cols = ['file_hash', 'filename', 'file_size_bytes', 'upload_date', 'account_email', 'device_source', 'remote_id', 'album_name']
        cols_str = ', '.join(cols)
        
        if self.cache_cursor:
            try:
                with self._sqlite_lock:
                    self.cache_cursor.execute(f"SELECT {cols_str} FROM media_library WHERE file_hash = ? LIMIT 1", (file_hash,))
                    row = self.cache_cursor.fetchone()
                    if row:
                        return dict(zip(cols, row))
            except Exception as e:
                logger.error(f"Local cache query failed: {e}")
                
        sql = f"SELECT {cols_str} FROM media_library WHERE file_hash = %s LIMIT 1"
        row = self.execute_query(sql, (file_hash,), fetch_one=True)
        if row: return dict(zip(cols, row))
        return None

    def add_filename_alias(self, file_hash: str, new_filename: str):
        """Appends a new filename to the existing comma-separated filename list for a given hash."""
        file_data = self.get_file_by_hash(file_hash)
        if not file_data:
            return

        current_names = file_data.get("filename", "")
        names_list = [n.strip() for n in current_names.split(',') if n.strip()]
        
        if new_filename not in names_list:
            names_list.append(new_filename)
            updated_names = ', '.join(names_list)
            
            sql = "UPDATE media_library SET filename = %s WHERE file_hash = %s"
            self.execute_query(sql, (updated_names, file_hash), is_write=True)
            
            if self.cache_cursor:
                with self._sqlite_lock:
                    self.cache_cursor.execute("UPDATE media_library SET filename = ? WHERE file_hash = ?", (updated_names, file_hash))
                    self.cache_conn.commit()
            logger.info(f"🏷️ Added filename alias '{new_filename}' to hash {file_hash[:8]}...")

    def insert_file(self, file_data: dict):
        """Inserts a new file record or updates it, and incrementally updates storage summary."""
        keys = []
        vals = []
        for k, v in file_data.items():
            if k not in ['id', 'sl_no']:
                keys.append(k)
                vals.append(v)
                
        cols_str = ', '.join(keys)
        placeholders = ', '.join(['%s'] * len(keys))
        
        # Use ON CONFLICT to UPDATE if already exists (Source of truth: local always wins on manual insert)
        update_cols = [k for k in keys if k != 'file_hash']
        update_str = ", ".join([f"{k} = EXCLUDED.{k}" for k in update_cols])
        
        sql = f"""
            INSERT INTO media_library ({cols_str}) 
            VALUES ({placeholders}) 
            ON CONFLICT (file_hash) DO UPDATE SET 
            {update_str}
            RETURNING sl_no, {cols_str}
        """
        row = self.execute_query(sql, tuple(vals), is_write=True, fetch_one=True)
        
        if row:
            if self.cache_cursor:
                returned_keys = ['sl_no'] + keys
                sqlite_placeholders = ', '.join(['?'] * len(returned_keys))
                sqlite_cols = ', '.join(returned_keys)
                # Use REPLACE for SQLite to update on conflict
                sqlite_insert = f"INSERT OR REPLACE INTO media_library ({sqlite_cols}) VALUES ({sqlite_placeholders})"
                with self._sqlite_lock:
                    self.cache_cursor.execute(sqlite_insert, row)
                    self.cache_conn.commit()
                    
            # Note: For simplicity, we only increment storage summary on new inserts in this helper.
            # If it's an update, the counts might not change, but sizes might. 
            # A full refresh periodically is still recommended.
            self.increment_storage_summary(file_data)

    def increment_storage_summary(self, file_data: dict):
        """Incrementally updates storage summary counters and sizes for a single file insertion."""
        try:
            filename = file_data.get("filename", "")
            size_bytes = file_data.get("file_size_bytes", 0)
            email = file_data.get("account_email")
            device = file_data.get("device_source")
            album_name = file_data.get("album_name")
            
            photo_exts = {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp', '.heic', '.heif', '.tiff'}
            video_exts = {'.mp4', '.mov', '.avi', '.mkv', '.wmv', '.flv', '.webm'}
            
            ext = os.path.splitext(filename or "")[1].lower()
            is_photo = ext in photo_exts
            is_video = ext in video_exts
            
            size_gb = size_bytes / (1024**3)
            size_mb = size_bytes / (1024**2)
            
            p_inc = 1 if is_photo else 0
            v_inc = 1 if is_video else 0
            ps_gb_inc = size_gb if is_photo else 0
            vs_gb_inc = size_gb if is_video else 0
            ps_mb_inc = size_mb if is_photo else 0
            vs_mb_inc = size_mb if is_video else 0

            # 1. Update Overall Summary (Atomic)
            sql_sum = """
                UPDATE storage_summary SET
                    total_photos = total_photos + %s,
                    total_videos = total_videos + %s,
                    total_assets = total_assets + 1,
                    total_photos_size_gb = total_photos_size_gb + %s,
                    total_videos_size_gb = total_videos_size_gb + %s,
                    total_size_gb = total_size_gb + %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = 1
            """
            self.execute_query(sql_sum, (p_inc, v_inc, ps_gb_inc, vs_gb_inc, size_gb), is_write=True)
            if self.cache_cursor:
                with self._sqlite_lock:
                    self.cache_cursor.execute(sql_sum.replace('%s', '?'), (p_inc, v_inc, ps_gb_inc, vs_gb_inc, size_gb))
                    self.cache_conn.commit()

            # 2. Update Account Distribution (Atomic UPSERT)
            if email:
                sql_acc = """
                    INSERT INTO account_distribution 
                    (summary_id, account_email, photos_count, videos_count, photos_size_mb, videos_size_mb, total_size_mb, percentage)
                    VALUES (1, %s, %s, %s, %s, %s, %s, 0)
                    ON CONFLICT (account_email) DO UPDATE SET
                        photos_count = account_distribution.photos_count + EXCLUDED.photos_count,
                        videos_count = account_distribution.videos_count + EXCLUDED.videos_count,
                        photos_size_mb = account_distribution.photos_size_mb + EXCLUDED.photos_size_mb,
                        videos_size_mb = account_distribution.videos_size_mb + EXCLUDED.videos_size_mb,
                        total_size_mb = account_distribution.total_size_mb + EXCLUDED.total_size_mb
                """
                self.execute_query(sql_acc, (email, p_inc, v_inc, ps_mb_inc, vs_mb_inc, size_mb), is_write=True)
                if self.cache_cursor:
                    with self._sqlite_lock:
                        # SQLite uses a slightly different syntax for self-reference in UPSERT
                        sqlite_acc = """
                            INSERT INTO account_distribution 
                            (summary_id, account_email, photos_count, videos_count, photos_size_mb, videos_size_mb, total_size_mb, percentage)
                            VALUES (1, ?, ?, ?, ?, ?, ?, 0)
                            ON CONFLICT (account_email) DO UPDATE SET
                                photos_count = photos_count + excluded.photos_count,
                                videos_count = videos_count + excluded.videos_count,
                                photos_size_mb = photos_size_mb + excluded.photos_size_mb,
                                videos_size_mb = videos_size_mb + excluded.videos_size_mb,
                                total_size_mb = total_size_mb + excluded.total_size_mb
                        """
                        self.cache_cursor.execute(sqlite_acc, (email, p_inc, v_inc, ps_mb_inc, vs_mb_inc, size_mb))
                        self.cache_conn.commit()

            # 3. Update Device Distribution (Atomic UPSERT)
            if device:
                sql_dev = """
                    INSERT INTO device_distribution 
                    (summary_id, device_name, photos_count, videos_count, photos_size_mb, videos_size_mb, total_size_mb, percentage)
                    VALUES (1, %s, %s, %s, %s, %s, %s, 0)
                    ON CONFLICT (device_name) DO UPDATE SET
                        photos_count = device_distribution.photos_count + EXCLUDED.photos_count,
                        videos_count = device_distribution.videos_count + EXCLUDED.videos_count,
                        photos_size_mb = device_distribution.photos_size_mb + EXCLUDED.photos_size_mb,
                        videos_size_mb = device_distribution.videos_size_mb + EXCLUDED.videos_size_mb,
                        total_size_mb = device_distribution.total_size_mb + EXCLUDED.total_size_mb
                """
                self.execute_query(sql_dev, (device, p_inc, v_inc, ps_mb_inc, vs_mb_inc, size_mb), is_write=True)
                if self.cache_cursor:
                    with self._sqlite_lock:
                        sqlite_dev = """
                            INSERT INTO device_distribution 
                            (summary_id, device_name, photos_count, videos_count, photos_size_mb, videos_size_mb, total_size_mb, percentage)
                            VALUES (1, ?, ?, ?, ?, ?, ?, 0)
                            ON CONFLICT (device_name) DO UPDATE SET
                                photos_count = photos_count + excluded.photos_count,
                                videos_count = videos_count + excluded.videos_count,
                                photos_size_mb = photos_size_mb + excluded.photos_size_mb,
                                videos_size_mb = videos_size_mb + excluded.videos_size_mb,
                                total_size_mb = total_size_mb + excluded.total_size_mb
                        """
                        self.cache_cursor.execute(sqlite_dev, (device, p_inc, v_inc, ps_mb_inc, vs_mb_inc, size_mb))
                        self.cache_conn.commit()
            
            # 4. Update Trip Metadata (Incremental)
            if album_name and (is_photo or is_video):
                # Fetch current metadata
                curr_meta = {}
                sql_get = "SELECT asset_metadata FROM trips_config WHERE name = %s"
                if self.cache_cursor:
                    with self._sqlite_lock:
                        self.cache_cursor.execute("SELECT asset_metadata FROM trips_config WHERE name = ?", (album_name,))
                        res = self.cache_cursor.fetchone()
                        if res and res[0]:
                            try:
                                curr_meta = json.loads(res[0])
                            except: pass
                else:
                    res = self.execute_query(sql_get, (album_name,), fetch_one=True)
                    if res and res[0]:
                        try:
                            # In PG, asset_metadata might be returned as dict if using some adapters, 
                            # but pg8000 usually returns it as string or dict depending on type.
                            curr_meta = res[0] if isinstance(res[0], dict) else json.loads(res[0])
                        except: pass
                
                # Update stats
                if 'photos' not in curr_meta: curr_meta['photos'] = 0
                if 'videos' not in curr_meta: curr_meta['videos'] = 0
                if 'photos_count' not in curr_meta: curr_meta['photos_count'] = 0
                if 'videos_count' not in curr_meta: curr_meta['videos_count'] = 0
                
                if is_photo: 
                    curr_meta['photos'] += size_bytes
                    curr_meta['photos_count'] += 1
                elif is_video: 
                    curr_meta['videos'] += size_bytes
                    curr_meta['videos_count'] += 1
                
                new_meta_json = json.dumps(curr_meta)
                sql_upd = "UPDATE trips_config SET asset_metadata = %s WHERE name = %s"
                self.execute_query(sql_upd, (new_meta_json, album_name), is_write=True)
                if self.cache_cursor:
                    with self._sqlite_lock:
                        self.cache_cursor.execute("UPDATE trips_config SET asset_metadata = ? WHERE name = ?", (new_meta_json, album_name))
                        self.cache_conn.commit()

            # Note: Percentages are left for the full refresh at startup to keep this fast.
            # They can also be recalculated here if strictly needed, but requires another READ.
            
        except Exception as e:
            logger.error(f"❌ Failed incremental storage update: {e}")

    def get_trips(self):
        """Fetches all active trips."""
        if self.cache_cursor:
            try:
                with self._sqlite_lock:
                    self.cache_cursor.execute("SELECT name, start, end, require_gps, album_id, album_url, email_message_id, asset_metadata FROM trips_config")
                    rows = self.cache_cursor.fetchall()
                    if rows:
                        return [{"name": r[0], "start": r[1], "end": r[2], "require_gps": bool(r[3]), "album_id": r[4], "album_url": r[5], "email_message_id": r[6], "asset_metadata": r[7]} for r in rows]
            except Exception as e:
                logger.error(f"❌ Failed to fetch trips locally: {e}")
                
        sql = "SELECT name, start, \"end\", require_gps, album_id, album_url, email_message_id, asset_metadata FROM trips_config"
        rows = self.execute_query(sql, fetch_all=True)
        if not rows: return []
        return [{"name": r[0], "start": r[1], "end": r[2], "require_gps": bool(r[3]), "album_id": r[4], "album_url": r[5], "email_message_id": r[6], "asset_metadata": r[7]} for r in rows]

    def update_trip_album_id(self, trip_name: str, album_id: str, album_url: str = None):
        """Updates the album ID and URL for a specific trip in both Cloud and Local Cache."""
        try:
            if album_url:
                sql = "UPDATE trips_config SET album_id = %s, album_url = %s WHERE name = %s"
                params = (album_id, album_url, trip_name)
                sqlite_sql = "UPDATE trips_config SET album_id = ?, album_url = ? WHERE name = ?"
                sqlite_params = (album_id, album_url, trip_name)
            else:
                sql = "UPDATE trips_config SET album_id = %s WHERE name = %s"
                params = (album_id, trip_name)
                sqlite_sql = "UPDATE trips_config SET album_id = ? WHERE name = ?"
                sqlite_params = (album_id, trip_name)

            self.execute_query(sql, params, is_write=True)
            
            if self.cache_cursor:
                with self._sqlite_lock:
                    self.cache_cursor.execute(sqlite_sql, sqlite_params)
                    self.cache_conn.commit()
                logger.info(f"💾 Updated Album ID/URL for trip '{trip_name}' in cache & cloud.")
        except Exception as e:
            logger.error(f"❌ Failed to update trip album ID: {e}")

    def update_trip_message_id(self, trip_name: str, message_id: str):
        """Stores the email Message-ID for a trip's album creation notification."""
        try:
            sql = "UPDATE trips_config SET email_message_id = %s WHERE name = %s"
            params = (message_id, trip_name)
            self.execute_query(sql, params, is_write=True)

            if self.cache_cursor:
                with self._sqlite_lock:
                    self.cache_cursor.execute("UPDATE trips_config SET email_message_id = ? WHERE name = ?", (message_id, trip_name))
                    self.cache_conn.commit()
                logger.info(f"💾 Updated email_message_id for trip '{trip_name}' in cache & cloud.")
        except Exception as e:
            logger.error(f"❌ Failed to update trip message ID: {e}")

    def get_device_directories(self, device_name: str) -> list:
        """Fetches the comma-separated directory string from device_config and returns a list."""
        paths = []
        if self.cache_conn: # Assuming self.use_local_cache means self.cache_conn is active
            try:
                with self._sqlite_lock:
                    cur = self.cache_conn.cursor()
                    cur.execute("SELECT directories FROM device_config WHERE device_name = ?", (device_name,))
                    res = cur.fetchone()
                    if res and res[0]:
                        dirs = res[0].split(',')
                        paths = [d.strip() for d in dirs if d.strip()]
                        return paths
            except sqlite3.Error as e:
                 logger.error(f"Local query error for device config: {e}")
        
        # Fallback to cloud
        sql = "SELECT directories FROM device_config WHERE device_name = %s LIMIT 1"
        try:
            res = self.execute_query(sql, (device_name,), fetch_one=True)
            if res and res[0]:
                dirs = res[0].split(',')
                paths = [d.strip() for d in dirs if d.strip()]
                return paths
        except Exception as e:
            logger.error(f"Cloud query error for device config: {e}")

        return paths

    def upsert_device_config_local(self, device_name: str, directories: str):
        """Updates device_config in the local cache under lock (for init_wizard and other single-threaded callers)."""
        if not self.cache_cursor:
            return
        try:
            with self._sqlite_lock:
                self.cache_cursor.execute(
                    "REPLACE INTO device_config (device_name, directories) VALUES (?, ?)",
                    (device_name, directories)
                )
                self.cache_conn.commit()
        except Exception as e:
            logger.error(f"Failed to upsert device_config in local cache: {e}")

    def backup_to_local_sqlite(self, backup_path: str):
        """Creates a snapshot backup of the local cache database."""
        if not self.cache_conn:
            logger.warning("No local cache to backup. Skipping.")
            return

        try:
            os.makedirs(os.path.dirname(backup_path), exist_ok=True)
            with self._sqlite_lock:
                with sqlite3.connect(backup_path) as backup_conn:
                    self.cache_conn.backup(backup_conn)
            logger.info(f"💾 Successfully backed up database to {backup_path}")
        except Exception as e:
            logger.error(f"❌ Failed to backup database: {e}")

    def check_connection(self):
        """Verifies connection status."""
        if self.provider_a_active or self.provider_b_active:
            status = []
            if self.provider_a_active: status.append("Nhost Active")
            if self.provider_b_active: status.append("Neon Active")
            logger.info("✅ Database Connections: " + " | ".join(status))
            return True
        return False
        
    def close(self):
        """Closes all active database connections cleanly."""
        if self.cache_conn:
            try:
                self.cache_conn.close()
                logger.info("💾 Local SQLite cache connection closed.")
            except Exception as e:
                logger.warning(f"⚠️ Error closing SQLite connection: {e}")
        if self.conn_a:
            try:
                self.conn_a.close()
            except Exception: pass
        if self.conn_b:
            try:
                self.conn_b.close()
            except Exception: pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False  # Do not suppress exceptions


# For backward compatibility with older scripts that reference DatabaseManager
DatabaseManager = DatabaseBalancer

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    try:
        db = DatabaseBalancer()
        if db.check_connection():
            print("Database connection successfully balanced.")
    except Exception as e:
        print(f"Init Error: {e}")
