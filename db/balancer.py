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

import pg8000.dbapi
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

        self._connect_providers()

        self.cache_conn = None
        self.cache_cursor = None
        if use_local_cache:
            self.init_local_cache()
            self.reconcile_databases()

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
        if "10054" in err_str or "10053" in err_str: return True
        if "forcibly closed" in err_str: return True
        if "network error" in err_str: return True
        if "broken pipe" in err_str: return True
        if "connection reset" in err_str: return True
        if "connection aborted" in err_str: return True
        if "interfaceerror" in err_str: return True
        if "closed" in err_str: return True
        if isinstance(e, (ConnectionError, OSError)): return True
        return False

    def _reconnect_provider(self, provider_id: str):
        if provider_id == 'A' and self.nhost_url:
            try:
                kwargs_a = self._parse_url(self.nhost_url)
                kwargs_a['tcp_keepalive'] = True
                self.conn_a = pg8000.dbapi.connect(**kwargs_a)
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
                kwargs_b = self._parse_url(self.neon_url)
                kwargs_b['tcp_keepalive'] = True
                self.conn_b = pg8000.dbapi.connect(**kwargs_b)
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
        for active, conn, name in [
            (self.provider_a_active, self.conn_a, "Nhost (A)"),
            (self.provider_b_active, self.conn_b, "Neon (B)")
        ]:
            if not active:
                continue
            try:
                cursor = conn.cursor()
                cursor.execute("ALTER TABLE trips_config ADD COLUMN IF NOT EXISTS email_message_id TEXT")
                cursor.execute("ALTER TABLE trips_config ADD COLUMN IF NOT EXISTS asset_metadata JSONB")
                
                # Ensure media_library has a unique constraint on file_hash
                # Remove duplicates first to avoid failure
                cursor.execute("""
                    DELETE FROM media_library a USING media_library b
                    WHERE a.sl_no > b.sl_no AND a.file_hash = b.file_hash
                """)
                try:
                    cursor.execute("ALTER TABLE media_library ADD CONSTRAINT media_library_file_hash_unique UNIQUE (file_hash)")
                except Exception: pass # Already exists or table empty
                
                # Create storage_summary and ensure ID=1 exists for increments
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS storage_summary (
                        id SERIAL PRIMARY KEY,
                        synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        total_photos INTEGER DEFAULT 0,
                        total_videos INTEGER DEFAULT 0,
                        total_assets INTEGER DEFAULT 0,
                        total_photos_size_gb REAL DEFAULT 0,
                        total_videos_size_gb REAL DEFAULT 0,
                        total_size_gb REAL DEFAULT 0
                    )
                """)
                cursor.execute("ALTER TABLE storage_summary ADD COLUMN IF NOT EXISTS total_photos_size_gb REAL DEFAULT 0")
                cursor.execute("ALTER TABLE storage_summary ADD COLUMN IF NOT EXISTS total_videos_size_gb REAL DEFAULT 0")
                
                # Defensive: Ensure ID is actually a PK or Unique for UPSERT
                try:
                    cursor.execute("ALTER TABLE storage_summary ADD CONSTRAINT storage_summary_id_unique UNIQUE (id)")
                except Exception: pass

                # Ensure row with ID=1 exists
                cursor.execute("INSERT INTO storage_summary (id) VALUES (1) ON CONFLICT (id) DO NOTHING")
                
                # Deduplicate and add UNIQUE constraint to account_distribution
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS account_distribution (
                        id SERIAL PRIMARY KEY,
                        summary_id INTEGER REFERENCES storage_summary(id) ON DELETE CASCADE,
                        account_email TEXT NOT NULL,
                        photos_count INTEGER DEFAULT 0,
                        videos_count INTEGER DEFAULT 0,
                        photos_size_mb REAL DEFAULT 0,
                        videos_size_mb REAL DEFAULT 0,
                        total_size_mb REAL DEFAULT 0,
                        percentage REAL DEFAULT 0
                    )
                """)
                # Remove duplicates before adding constraint
                cursor.execute("""
                    DELETE FROM account_distribution a USING account_distribution b
                    WHERE a.id < b.id AND a.account_email = b.account_email
                """)
                try:
                    cursor.execute("ALTER TABLE account_distribution ADD CONSTRAINT acc_dist_email_unique UNIQUE (account_email)")
                except Exception: pass
                
                # Deduplicate and add UNIQUE constraint to device_distribution
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS device_distribution (
                        id SERIAL PRIMARY KEY,
                        summary_id INTEGER REFERENCES storage_summary(id) ON DELETE CASCADE,
                        device_name TEXT NOT NULL,
                        photos_count INTEGER DEFAULT 0,
                        videos_count INTEGER DEFAULT 0,
                        total_size_mb REAL DEFAULT 0,
                        percentage REAL DEFAULT 0
                    )
                """)
                # Remove duplicates before adding constraint
                cursor.execute("""
                    DELETE FROM device_distribution a USING device_distribution b
                    WHERE a.id < b.id AND a.device_name = b.device_name
                """)
                try:
                    cursor.execute("ALTER TABLE device_distribution ADD CONSTRAINT dev_dist_name_unique UNIQUE (device_name)")
                except Exception: pass
                
                conn.commit()
                logger.debug(f"✅ Ensured schema on {name}")
            except Exception as e:
                logger.warning(f"⚠️ Could not migrate schema on {name}: {e}")

    def refresh_storage_summary(self, use_local_for_calc=False):
        """Recalculates storage statistics and updates summary tables surgically."""
        logger.info(f"📊 Refreshing storage summary stats (using {'Local' if use_local_for_calc else 'Cloud'} for calc)...")
        
        photo_exts = {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp', '.heic', '.heif', '.tiff'}
        video_exts = {'.mp4', '.mov', '.avi', '.mkv', '.wmv', '.flv', '.webm'}

        try:
            # 1. Fetch current data for calculation
            sql = "SELECT filename, file_size_bytes, account_email, device_source, album_name FROM media_library"
            if use_local_for_calc and self.cache_cursor:
                with self._sqlite_lock:
                    self.cache_cursor.execute(sql.replace('album_name', 'album_name')) # Ensure album_name is there
                    rows = self.cache_cursor.fetchall()
            else:
                rows = self.execute_query(sql, fetch_all=True)

            if not rows:
                logger.warning("No media records found to summarize.")
                return

            total_photos = 0
            total_videos = 0
            total_photos_size_bytes = 0
            total_videos_size_bytes = 0
            total_size_bytes = 0
            
            accounts_data = {} # email -> {p:0, v:0, ps:0, vs:0}
            devices_data = {}  # device -> {p:0, v:0, s:0}
            
            # Initialize trip stats
            all_trips = self.get_trips()
            trips_stats = {t['name']: {"photos": 0, "videos": 0, "photos_count": 0, "videos_count": 0} for t in all_trips}

            for filename, size, email, device, album_name in rows:
                ext = os.path.splitext(filename or "")[1].lower()
                is_photo = ext in photo_exts
                is_video = ext in video_exts
                
                size_val = size or 0
                total_size_bytes += size_val
                if is_photo: 
                    total_photos += 1
                    total_photos_size_bytes += size_val
                elif is_video: 
                    total_videos += 1
                    total_videos_size_bytes += size_val
                
                # Account stats
                if email not in accounts_data:
                    accounts_data[email] = {'p': 0, 'v': 0, 'ps': 0, 'vs': 0}
                if is_photo:
                    accounts_data[email]['p'] += 1
                    accounts_data[email]['ps'] += size_val
                elif is_video:
                    accounts_data[email]['v'] += 1
                    accounts_data[email]['vs'] += size_val

                # Device stats
                if device not in devices_data:
                    devices_data[device] = {'p': 0, 'v': 0, 's': 0}
                devices_data[device]['s'] += size_val
                if is_photo: devices_data[device]['p'] += 1
                elif is_video: devices_data[device]['v'] += 1
                
                # Trip stats
                if album_name in trips_stats:
                    if is_photo:
                        trips_stats[album_name]["photos"] += size_val
                        trips_stats[album_name]["photos_count"] += 1
                    elif is_video:
                        trips_stats[album_name]["videos"] += size_val
                        trips_stats[album_name]["videos_count"] += 1

            total_assets = total_photos + total_videos
            total_size_gb = total_size_bytes / (1024**3)
            total_photos_size_gb = total_photos_size_bytes / (1024**3)
            total_videos_size_gb = total_videos_size_bytes / (1024**3)
            total_size_mb_all = total_size_bytes / (1024**2)

            # --- Surgical Update Logic ---
            # We use summary_id = 1 for the "latest state" record
            summary_id = 1

            # 2. Check & Update Overall Summary
            needs_summary_update = True
            if self.cache_cursor:
                with self._sqlite_lock:
                    self.cache_cursor.execute("SELECT total_photos, total_videos, total_assets, total_photos_size_gb, total_videos_size_gb, total_size_gb FROM storage_summary WHERE id = 1")
                    last = self.cache_cursor.fetchone()
                    if last:
                        if (total_photos == last[0] and total_videos == last[1] and total_assets == last[2] and 
                            abs(total_photos_size_gb - last[3]) < 1e-6 and abs(total_videos_size_gb - last[4]) < 1e-6 and abs(total_size_gb - last[5]) < 1e-6):
                            needs_summary_update = False

            if needs_summary_update:
                sql_sum = """
                    INSERT INTO storage_summary (id, total_photos, total_videos, total_assets, total_photos_size_gb, total_videos_size_gb, total_size_gb)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO UPDATE SET
                        total_photos = EXCLUDED.total_photos,
                        total_videos = EXCLUDED.total_videos,
                        total_assets = EXCLUDED.total_assets,
                        total_photos_size_gb = EXCLUDED.total_photos_size_gb,
                        total_videos_size_gb = EXCLUDED.total_videos_size_gb,
                        total_size_gb = EXCLUDED.total_size_gb,
                        synced_at = CURRENT_TIMESTAMP
                """
                self.execute_query(sql_sum, (summary_id, total_photos, total_videos, total_assets, total_photos_size_gb, total_videos_size_gb, total_size_gb), is_write=True)
                if self.cache_cursor:
                    with self._sqlite_lock:
                        self.cache_cursor.execute(sql_sum.replace('%s', '?'), (summary_id, total_photos, total_videos, total_assets, total_photos_size_gb, total_videos_size_gb, total_size_gb))
                        self.cache_conn.commit()

            # 3. Check & Update Account Distribution
            for email, data in accounts_data.items():
                ps_mb = data['ps'] / (1024**2)
                vs_mb = data['vs'] / (1024**2)
                t_mb = ps_mb + vs_mb
                pct = (t_mb / total_size_mb_all * 100) if total_size_mb_all > 0 else 0
                
                needs_acc_update = True
                if self.cache_cursor:
                    with self._sqlite_lock:
                        self.cache_cursor.execute("SELECT photos_count, videos_count, photos_size_mb, videos_size_mb, total_size_mb, percentage FROM account_distribution WHERE account_email = ?", (email,))
                        last_acc = self.cache_cursor.fetchone()
                        if last_acc:
                            if (data['p'] == last_acc[0] and data['v'] == last_acc[1] and 
                                abs(ps_mb - last_acc[2]) < 1e-4 and abs(vs_mb - last_acc[3]) < 1e-4 and abs(t_mb - last_acc[4]) < 1e-4 and abs(pct - last_acc[5]) < 1e-4):
                                needs_acc_update = False

                if needs_acc_update:
                    sql_acc = """
                        INSERT INTO account_distribution (summary_id, account_email, photos_count, videos_count, photos_size_mb, videos_size_mb, total_size_mb, percentage)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (account_email) DO UPDATE SET
                            photos_count = EXCLUDED.photos_count,
                            videos_count = EXCLUDED.videos_count,
                            photos_size_mb = EXCLUDED.photos_size_mb,
                            videos_size_mb = EXCLUDED.videos_size_mb,
                            total_size_mb = EXCLUDED.total_size_mb,
                            percentage = EXCLUDED.percentage
                    """
                    self.execute_query(sql_acc, (summary_id, email, data['p'], data['v'], ps_mb, vs_mb, t_mb, pct), is_write=True)
                    if self.cache_cursor:
                        with self._sqlite_lock:
                            self.cache_cursor.execute(sql_acc.replace('%s', '?'), (summary_id, email, data['p'], data['v'], ps_mb, vs_mb, t_mb, pct))
                            self.cache_conn.commit()

            # 4. Check & Update Device Distribution
            for device, data in devices_data.items():
                t_mb = data['s'] / (1024**2)
                pct = (t_mb / total_size_mb_all * 100) if total_size_mb_all > 0 else 0
                
                needs_dev_update = True
                if self.cache_cursor:
                    with self._sqlite_lock:
                        self.cache_cursor.execute("SELECT photos_count, videos_count, total_size_mb, percentage FROM device_distribution WHERE device_name = ?", (device,))
                        last_dev = self.cache_cursor.fetchone()
                        if last_dev:
                            if (data['p'] == last_dev[0] and data['v'] == last_dev[1] and abs(t_mb - last_dev[2]) < 1e-4 and abs(pct - last_dev[3]) < 1e-4):
                                needs_dev_update = False

                if needs_dev_update:
                    sql_dev = """
                        INSERT INTO device_distribution (summary_id, device_name, photos_count, videos_count, total_size_mb, percentage)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (device_name) DO UPDATE SET
                            photos_count = EXCLUDED.photos_count,
                            videos_count = EXCLUDED.videos_count,
                            total_size_mb = EXCLUDED.total_size_mb,
                            percentage = EXCLUDED.percentage
                    """
                    self.execute_query(sql_dev, (summary_id, device, data['p'], data['v'], t_mb, pct), is_write=True)
                    if self.cache_cursor:
                        with self._sqlite_lock:
                            self.cache_cursor.execute(sql_dev.replace('%s', '?'), (summary_id, device, data['p'], data['v'], t_mb, pct))
                            self.cache_conn.commit()
            
            # 5. Update Trip Metadata
            for t_name, stats in trips_stats.items():
                meta_json = json.dumps(stats)
                sql_trip = "UPDATE trips_config SET asset_metadata = %s WHERE name = %s"
                self.execute_query(sql_trip, (meta_json, t_name), is_write=True)
                if self.cache_cursor:
                    with self._sqlite_lock:
                        self.cache_cursor.execute("UPDATE trips_config SET asset_metadata = ? WHERE name = ?", (meta_json, t_name))
                        self.cache_conn.commit()

            logger.info("✅ Storage summary refresh complete (Surgical).")

        except Exception as e:
            logger.error(f"❌ Failed to refresh storage summary: {e}")

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

    def _reconcile_table(self, table_name: str, cache_table_name_for_sqlite: str = None, pk_col_for_max: str = "sl_no"):
        """Helper to reconcile a specific table using MAX(incrementing_col)."""
        if not cache_table_name_for_sqlite:
            cache_table_name_for_sqlite = table_name

        max_a = 0
        max_b = 0

        if self.provider_a_active and self.conn_a:
            try:
                cursor_a = self.conn_a.cursor()
                cursor_a.execute(f"SELECT MAX({pk_col_for_max}) FROM {table_name}")
                res_a = cursor_a.fetchone()
                if res_a and res_a[0]: max_a = res_a[0]
            except Exception as e:
                logger.error(f"Failed to query Max {pk_col_for_max} from A for {table_name}: {e}")

        if self.provider_b_active and self.conn_b:
            try:
                cursor_b = self.conn_b.cursor()
                cursor_b.execute(f"SELECT MAX({pk_col_for_max}) FROM {table_name}")
                res_b = cursor_b.fetchone()
                if res_b and res_b[0]: max_b = res_b[0]
            except Exception as e:
                logger.error(f"Failed to query Max {pk_col_for_max} from B for {table_name}: {e}")
        
        if max_a == max_b:
            logger.info(f"✅ {table_name} - Both providers in sync (Max {pk_col_for_max}: {max_a}).")
            return
            
        logger.warning(f"⚠️ {table_name} - Mismatch detected! Nhost(A): {max_a}, Neon(B): {max_b}")
        
        if max_a > max_b:
            leading_conn = self.conn_a
            lagging_conn = self.conn_b
            leading_name = "Nhost(A)"
            lagging_name = "Neon(B)"
            leading_max = max_a
            lagging_max = max_b
        else:
            leading_conn = self.conn_b
            lagging_conn = self.conn_a
            leading_name = "Neon(B)"
            lagging_name = "Nhost(A)"
            leading_max = max_b
            lagging_max = max_a
            
        logger.info(f"{table_name} - Leader is {leading_name}, Lagger is {lagging_name}. Fetching missing rows...")
        
        try:
            cursor_lead = leading_conn.cursor()
            cursor_lead.execute(f"SELECT * FROM {table_name} WHERE {pk_col_for_max} > %s ORDER BY {pk_col_for_max} ASC", (lagging_max,))
            missing_rows = cursor_lead.fetchall()
            
            if not missing_rows:
                return
                
            col_names = [desc[0] for desc in cursor_lead.description]
            cursor_lag = lagging_conn.cursor()
            placeholders = ', '.join(['%s'] * len(col_names))
            cols_str = ', '.join(['"' + c + '"' if c in ["end"] else c for c in col_names])
            
            pk_map = {
                "media_library": "sl_no",
                "trips_config": "name",
                "device_config": "device_name",
                "storage_summary": "id",
                "account_distribution": "id",
                "device_distribution": "id"
            }
            pk_col = pk_map.get(table_name)
            
            if pk_col:
                update_cols = [c for c in col_names if c != pk_col]
                if update_cols:
                    update_str = ", ".join([f'"{c}" = EXCLUDED."{c}"' if c in ["end", "order"] else f"{c} = EXCLUDED.{c}" for c in update_cols])
                    insert_sql = f"INSERT INTO {table_name} ({cols_str}) VALUES ({placeholders}) ON CONFLICT ({pk_col}) DO UPDATE SET {update_str}"
                else:
                    insert_sql = f"INSERT INTO {table_name} ({cols_str}) VALUES ({placeholders}) ON CONFLICT ({pk_col}) DO NOTHING"
            else:
                insert_sql = f"INSERT INTO {table_name} ({cols_str}) VALUES ({placeholders})"
            
            for row in tqdm(missing_rows, desc=f"Syncing {table_name} to {lagging_name}", unit="rows"):
                cursor_lag.execute(insert_sql, row)
                
            if self.cache_cursor:
                    sqlite_placeholders = ', '.join(['?'] * len(col_names))
                    sqlite_insert = f"REPLACE INTO {cache_table_name_for_sqlite} ({cols_str}) VALUES ({sqlite_placeholders})"
                    with self._sqlite_lock:
                        for row in tqdm(missing_rows, desc=f"Syncing {table_name} to local cache", unit="rows"):
                            self.cache_cursor.execute(sqlite_insert, row)
                        self.cache_conn.commit()
                
            subject = f"Recovery Successful - {table_name}"
            body = f"Reconciled {table_name} databases.\nIdentified {lagging_name} as lagging by {len(missing_rows)} rows.\nSynced rows successfully to {lagging_name} and local cache."
            logger.info(f"✅ {body}")
            send_notification_email(subject, body)
            
        except Exception as e:
            logger.error(f"❌ Failed to reconcile {table_name} databases: {e}")

    def reconcile_databases(self):
        """Self-Heal Phase: Reconciles databases using max(sl_no) or max(id)."""
        logger.info("🔍 Running Initialization & Auto-Reconciliation...")
        if not self.provider_a_active or not self.provider_b_active:
            logger.info("One or both providers offline. Skipping full reconciliation.")
            self._sync_sequences()
            return

        self._reconcile_table("media_library")
        self._reconcile_table("trips_config")
        self._reconcile_table("device_config")
        
        # Sync summary tables (in order of dependencies)
        self._reconcile_table("storage_summary", pk_col_for_max="id")
        self._reconcile_table("account_distribution", pk_col_for_max="id")
        self._reconcile_table("device_distribution", pk_col_for_max="id")
            
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
            self.cache_conn.execute('''
                CREATE TABLE IF NOT EXISTS media_library (
                    sl_no INTEGER PRIMARY KEY,
                    file_hash TEXT NOT NULL UNIQUE,
                    filename TEXT,
                    file_size_bytes INTEGER,
                    upload_date TEXT,
                    account_email TEXT,
                    device_source TEXT,
                    remote_id TEXT,
                    album_name TEXT
                )
            ''')
            # Deduplicate SQLite media_library before ensuring unique index
            try:
                self.cache_conn.execute("""
                    DELETE FROM media_library 
                    WHERE sl_no NOT IN (
                        SELECT MIN(sl_no) 
                        FROM media_library 
                        GROUP BY file_hash
                    )
                """)
                self.cache_conn.commit()
            except sqlite3.OperationalError: pass

            self.cache_conn.execute("CREATE INDEX IF NOT EXISTS idx_filename ON media_library(filename)")
            self.cache_conn.execute("CREATE INDEX IF NOT EXISTS idx_filename_nocase ON media_library(filename COLLATE NOCASE)")
            self.cache_conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_hash ON media_library(file_hash)")
            
            self.cache_conn.execute('''
                CREATE TABLE IF NOT EXISTS trips_config (
                    sl_no INTEGER,
                    name TEXT PRIMARY KEY,
                    start TEXT,
                    "end" TEXT,
                    require_gps BOOLEAN,
                    album_id TEXT,
                    album_url TEXT,
                    email_message_id TEXT,
                    asset_metadata TEXT
                )
            ''')
            self.cache_conn.execute('''
                CREATE TABLE IF NOT EXISTS device_config (
                    device_name TEXT PRIMARY KEY,
                    directories TEXT,
                    sl_no INTEGER
                )
            ''')
            
            # New summary tables for SQLite
            self.cache_conn.execute('''
                CREATE TABLE IF NOT EXISTS storage_summary (
                    id INTEGER PRIMARY KEY,
                    synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    total_photos INTEGER DEFAULT 0,
                    total_videos INTEGER DEFAULT 0,
                    total_assets INTEGER DEFAULT 0,
                    total_photos_size_gb REAL DEFAULT 0,
                    total_videos_size_gb REAL DEFAULT 0,
                    total_size_gb REAL DEFAULT 0
                )
            ''')
            try:
                self.cache_conn.execute("ALTER TABLE storage_summary ADD COLUMN total_photos_size_gb REAL DEFAULT 0")
            except sqlite3.OperationalError: pass
            try:
                self.cache_conn.execute("ALTER TABLE storage_summary ADD COLUMN total_videos_size_gb REAL DEFAULT 0")
            except sqlite3.OperationalError: pass
            
            self.cache_conn.execute('''
                CREATE TABLE IF NOT EXISTS account_distribution (
                    id INTEGER PRIMARY KEY,
                    summary_id INTEGER REFERENCES storage_summary(id) ON DELETE CASCADE,
                    account_email TEXT NOT NULL,
                    photos_count INTEGER DEFAULT 0,
                    videos_count INTEGER DEFAULT 0,
                    photos_size_mb REAL DEFAULT 0,
                    videos_size_mb REAL DEFAULT 0,
                    total_size_mb REAL DEFAULT 0,
                    percentage REAL DEFAULT 0
                )
            ''')
            # Deduplicate and add UNIQUE index to account_distribution in SQLite
            try:
                self.cache_conn.execute("DELETE FROM account_distribution WHERE id NOT IN (SELECT MIN(id) FROM account_distribution GROUP BY account_email)")
                self.cache_conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_acc_email ON account_distribution(account_email)")
            except sqlite3.OperationalError: pass

            self.cache_conn.execute('''
                CREATE TABLE IF NOT EXISTS device_distribution (
                    id INTEGER PRIMARY KEY,
                    summary_id INTEGER REFERENCES storage_summary(id) ON DELETE CASCADE,
                    device_name TEXT NOT NULL,
                    photos_count INTEGER DEFAULT 0,
                    videos_count INTEGER DEFAULT 0,
                    total_size_mb REAL DEFAULT 0,
                    percentage REAL DEFAULT 0
                )
            ''')
            # Deduplicate and add UNIQUE index to device_distribution in SQLite
            try:
                self.cache_conn.execute("DELETE FROM device_distribution WHERE id NOT IN (SELECT MIN(id) FROM device_distribution GROUP BY device_name)")
                self.cache_conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_dev_name ON device_distribution(device_name)")
            except sqlite3.OperationalError: pass

            self.cache_conn.commit()
            
            # Migration: add columns to existing tables if they don't exist
            try:
                self.cache_conn.execute("ALTER TABLE device_config ADD COLUMN sl_no INTEGER")
                self.cache_conn.commit()
            except sqlite3.OperationalError:
                pass # Column already exists

            try:
                self.cache_conn.execute("ALTER TABLE trips_config ADD COLUMN album_url TEXT")
                self.cache_conn.commit()
            except sqlite3.OperationalError:
                pass # Column already exists

            try:
                self.cache_conn.execute("ALTER TABLE trips_config ADD COLUMN email_message_id TEXT")
                self.cache_conn.commit()
            except sqlite3.OperationalError:
                pass # Column already exists
                
            logger.info(f"✅ Local Cache initialized at {cache_path}")
            
        except Exception as e:
            logger.error(f"❌ Failed to init local cache: {e}")

    def sync_cloud_to_local(self):
        """Downloads ALL data from Cloud to Local Cache."""
        if not self.cache_conn: return
        
        logger.info("🔄 Syncing Cloud DB to Local Cache...")
        try:
            max_sl_no = 0
            with self._sqlite_lock:
                if self.cache_cursor:
                    self.cache_cursor.execute("SELECT MAX(sl_no) FROM media_library")
                    res = self.cache_cursor.fetchone()
                    if res and res[0] is not None:
                        max_sl_no = res[0]
                    
            sql = "SELECT sl_no, file_hash, filename, file_size_bytes, upload_date, account_email, device_source, remote_id, album_name FROM media_library WHERE sl_no > %s ORDER BY sl_no ASC"
            rows = self.execute_query(sql, (max_sl_no,), fetch_all=True)
            
            if rows:
                with self._sqlite_lock:
                    for row in rows:
                        self.cache_cursor.execute("""
                            REPLACE INTO media_library 
                            (sl_no, file_hash, filename, file_size_bytes, upload_date, account_email, device_source, remote_id, album_name)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, row)
                    self.cache_conn.commit()
                
            logger.info(f"✅ Sync Complete. {len(rows) if rows else 0} new records.")
            
            # Full sync of trips_config (replaces incremental, always authoritative)
            try:
                trips_rows = self.execute_query('SELECT name, start, "end", require_gps, album_id, album_url, email_message_id, asset_metadata FROM trips_config', fetch_all=True)

                with self._sqlite_lock:
                    self.cache_conn.execute("DELETE FROM trips_config")

                    if trips_rows:
                        for row in trips_rows:
                            is_gps_int = 1 if row[3] else 0
                            self.cache_conn.execute('''
                                INSERT INTO trips_config (name, start, "end", require_gps, album_id, album_url, email_message_id, asset_metadata)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                            ''', (row[0], row[1], row[2], is_gps_int, row[4], row[5], row[6], row[7]))
                        self.cache_conn.commit()
                        logger.info(f"💾 Synced {len(trips_rows)} trips configurations to local cache.")
                
                # Sync device_config (full sync)
                device_rows = self.execute_query('SELECT device_name, directories, sl_no FROM device_config', fetch_all=True)
                
                self.cache_conn.execute("DELETE FROM device_config")
                
                if device_rows:
                    for row in device_rows:
                        self.cache_conn.execute('''
                            INSERT INTO device_config (device_name, directories, sl_no)
                            VALUES (?, ?, ?)
                        ''', (row[0], row[1], row[2]))
                    self.cache_conn.commit()
                    logger.info(f"💾 Synced {len(device_rows)} device configurations to local cache.")
            except Exception as e:
                logger.warning(f"⚠️ Could not sync secondary configs: {e}")
                
            # Verify row counts after sync
            cloud_count = 0
            local_count = 0
            
            # Get Cloud count
            count_sql = "SELECT COUNT(*) FROM media_library"
            cloud_res = self.execute_query(count_sql, fetch_one=True)
            if cloud_res and cloud_res[0] is not None:
                cloud_count = cloud_res[0]
                
            # Get Local count
            if self.cache_cursor:
                with self._sqlite_lock:
                    self.cache_cursor.execute("SELECT COUNT(*) FROM media_library")
                    local_res = self.cache_cursor.fetchone()
                    if local_res and local_res[0] is not None:
                        local_count = local_res[0]
                    
            if cloud_count == local_count:
                logger.info(f"✅ Local database is fully synchronized. (Total Rows: {local_count})")
            else:
                logger.warning(f"⚠️ Row count mismatch after sync! Cloud: {cloud_count}, Local: {local_count}. Local database may be incomplete.")
                
        except Exception as e:
            logger.error(f"❌ Sync failed: {e}")

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

    def remove_photo_from_album_record(self, remote_id: str, album_name: str):
        """Removes the album association for a specific photo in both cloud and local cache."""
        try:
            sql = "UPDATE media_library SET album_name = NULL WHERE remote_id = %s AND album_name = %s"
            self.execute_query(sql, (remote_id, album_name), is_write=True)
            
            if self.cache_cursor:
                with self._sqlite_lock:
                    self.cache_cursor.execute("UPDATE media_library SET album_name = NULL WHERE remote_id = ? AND album_name = ?", (remote_id, album_name))
                    self.cache_conn.commit()
                logger.debug(f"🗑️ Removed album association for {remote_id} from cache & cloud.")
        except Exception as e:
            logger.error(f"❌ Failed to remove photo from album record: {e}")

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
        """Inserts a new file record and incrementally updates storage summary."""
        keys = []
        vals = []
        for k, v in file_data.items():
            if k not in ['id', 'sl_no']:
                keys.append(k)
                vals.append(v)
                
        cols_str = ', '.join(keys)
        placeholders = ', '.join(['%s'] * len(keys))
        
        # Use ON CONFLICT to ignore duplicates gracefully at the database level
        sql = f"INSERT INTO media_library ({cols_str}) VALUES ({placeholders}) ON CONFLICT (file_hash) DO NOTHING RETURNING sl_no, {cols_str}"
        row = self.execute_query(sql, tuple(vals), is_write=True, fetch_one=True)
        
        if row:
            if self.cache_cursor:
                returned_keys = ['sl_no'] + keys
                sqlite_placeholders = ', '.join(['?'] * len(returned_keys))
                sqlite_cols = ', '.join(returned_keys)
                # Use INSERT OR IGNORE for SQLite to respect the UNIQUE constraint
                sqlite_insert = f"INSERT OR IGNORE INTO media_library ({sqlite_cols}) VALUES ({sqlite_placeholders})"
                with self._sqlite_lock:
                    self.cache_cursor.execute(sqlite_insert, row)
                    self.cache_conn.commit()
                    
            # Trigger incremental summary update only if a new row was actually inserted
            self.increment_storage_summary(file_data)
        else:
            logger.debug(f"File {file_data.get('file_hash')[:8]} already exists in DB, skipped insert.")

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
                    synced_at = CURRENT_TIMESTAMP
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
                    (summary_id, account_email, photos_count, videos_count, photos_size_mb, videos_size_mb, total_size_mb)
                    VALUES (1, %s, %s, %s, %s, %s, %s)
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
                            (summary_id, account_email, photos_count, videos_count, photos_size_mb, videos_size_mb, total_size_mb)
                            VALUES (1, ?, ?, ?, ?, ?, ?)
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
                    (summary_id, device_name, photos_count, videos_count, total_size_mb)
                    VALUES (1, %s, %s, %s, %s)
                    ON CONFLICT (device_name) DO UPDATE SET
                        photos_count = device_distribution.photos_count + EXCLUDED.photos_count,
                        videos_count = device_distribution.videos_count + EXCLUDED.videos_count,
                        total_size_mb = device_distribution.total_size_mb + EXCLUDED.total_size_mb
                """
                self.execute_query(sql_dev, (device, p_inc, v_inc, size_mb), is_write=True)
                if self.cache_cursor:
                    with self._sqlite_lock:
                        sqlite_dev = """
                            INSERT INTO device_distribution 
                            (summary_id, device_name, photos_count, videos_count, total_size_mb)
                            VALUES (1, ?, ?, ?, ?)
                            ON CONFLICT (device_name) DO UPDATE SET
                                photos_count = photos_count + excluded.photos_count,
                                videos_count = videos_count + excluded.videos_count,
                                total_size_mb = total_size_mb + excluded.total_size_mb
                        """
                        self.cache_cursor.execute(sqlite_dev, (device, p_inc, v_inc, size_mb))
                        self.cache_conn.commit()
            
            # 4. Update Trip Metadata (Incremental)
            if album_name:
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
