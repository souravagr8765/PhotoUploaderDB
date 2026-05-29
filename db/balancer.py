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

# Logger setup
logger = lg

class DatabaseBalancer:
    def __init__(self, use_local_cache=False):
        self.nhost_url = get_config("database.nhost_url")
        self.neon_url = get_config("database.neon_url")
        self.nhost_enabled = get_config("database.nhost_enabled", True)
        self.neon_enabled = get_config("database.neon_enabled", True)

        self.provider_a_active = False
        self.provider_b_active = False
        self.conn_a = None
        self.conn_b = None

        self._sqlite_lock = threading.Lock()
        self._stats_queue = queue.Queue()
        self._worker_thread = threading.Thread(target=self._stats_worker, daemon=True)
        self._worker_thread.start()

        self._connect_providers()

        self.cache_conn = None
        self.cache_cursor = None
        if use_local_cache:
            self.init_local_cache()
            self.reconcile_databases()

    def _is_connection_error(self, e: Exception) -> bool:
        err_str = str(e).lower()
        return any(x in err_str for x in ["closed", "connection reset", "broken pipe", "eof"])

    def _reconnect_provider(self, provider_id: str):
        url = self.nhost_url if provider_id == 'A' else self.neon_url
        if url:
            try:
                # psycopg2 handles postgres:// and postgresql:// natively
                conn = psycopg2.connect(url)
                conn.autocommit = True
                if provider_id == 'A':
                    self.conn_a = conn
                    self.provider_a_active = True
                    logger.info("✅ Connected to Nhost (Provider A).")
                else:
                    self.conn_b = conn
                    self.provider_b_active = True
                    logger.info("✅ Connected to Neon (Provider B).")
                return True
            except Exception as e:
                if provider_id == 'A': self.provider_a_active = False
                else: self.provider_b_active = False
                logger.error(f"❌ {provider_id} Connection Failed: {e}")
        return False

    def _stats_worker(self):
        while True:
            try:
                task = self._stats_queue.get()
                if task is None: break
                func_name, args = task
                try:
                    if func_name == "increment_storage_summary": self._do_increment_storage_summary(*args)
                    elif func_name == "refresh_storage_summary": self._do_refresh_storage_summary(*args)
                except Exception as e:
                    logger.error(f"Background worker error: {e}")
                finally: self._stats_queue.task_done()
            except: time.sleep(1)

    def _migrate_cloud_schema(self):
        CURRENT_VERSION = 1
        CLOUD_SCHEMA = {
            "schema_info": {"id": "INTEGER PRIMARY KEY", "version": "INTEGER", "updated_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"},
            "media_library": {"sl_no": "SERIAL PRIMARY KEY", "file_hash": "TEXT NOT NULL", "filename": "TEXT", "file_size_bytes": "BIGINT", "upload_date": "TEXT", "account_email": "TEXT", "device_source": "TEXT", "remote_id": "TEXT", "album_name": "TEXT", "updated_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"},
            "sync_tracker": {"id": "INTEGER PRIMARY KEY", "last_sync_time": "TIMESTAMP DEFAULT '1970-01-01 00:00:00'"},
            "trips_config": {"name": "TEXT PRIMARY KEY", "sl_no": "SERIAL", "start": "TEXT", "end": "TEXT", "require_gps": "BOOLEAN DEFAULT FALSE", "album_id": "TEXT", "album_url": "TEXT", "email_message_id": "TEXT", "asset_metadata": "JSONB", "updated_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"},
            "device_config": {"device_name": "TEXT PRIMARY KEY", "directories": "TEXT", "sl_no": "SERIAL", "updated_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"},
            "storage_summary": {"id": "SERIAL PRIMARY KEY", "total_photos": "INTEGER DEFAULT 0", "total_videos": "INTEGER DEFAULT 0", "total_assets": "INTEGER DEFAULT 0", "total_photos_size_gb": "REAL DEFAULT 0", "total_videos_size_gb": "REAL DEFAULT 0", "total_size_gb": "REAL DEFAULT 0", "updated_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"},
            "account_distribution": {"id": "SERIAL PRIMARY KEY", "account_email": "TEXT NOT NULL", "photos_count": "INTEGER DEFAULT 0", "videos_count": "INTEGER DEFAULT 0", "photos_size_mb": "REAL DEFAULT 0", "videos_size_mb": "REAL DEFAULT 0", "total_size_mb": "REAL DEFAULT 0", "percentage": "REAL DEFAULT 0", "updated_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"},
            "device_distribution": {"device_name": "TEXT PRIMARY KEY", "photos_count": "INTEGER DEFAULT 0", "videos_count": "INTEGER DEFAULT 0", "photos_size_mb": "REAL DEFAULT 0", "videos_size_mb": "REAL DEFAULT 0", "total_size_mb": "REAL DEFAULT 0", "percentage": "REAL DEFAULT 0", "updated_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"}
        }
        for active, conn, name in [(self.provider_a_active, self.conn_a, "Nhost (A)"), (self.provider_b_active, self.conn_b, "Neon (B)")]:
            if not active: continue
            try:
                with conn.cursor() as cursor:
                    try:
                        cursor.execute("SELECT version FROM schema_info WHERE id = 1")
                        res = cursor.fetchone()
                        if res and res[0] == CURRENT_VERSION: continue
                    except: conn.rollback()

                    logger.info(f"🚀 Migrating {name}...")
                    for table, columns in CLOUD_SCHEMA.items():
                        pk = next(c for c, d in columns.items() if "PRIMARY KEY" in d)
                        cursor.execute(f'CREATE TABLE IF NOT EXISTS "{table}" ("{pk}" {columns[pk]})')
                        for c, d in columns.items():
                            if c != pk: cursor.execute(f'ALTER TABLE "{table}" ADD COLUMN IF NOT EXISTS "{c}" {d}')
                    cursor.execute("INSERT INTO storage_summary (id) VALUES (1) ON CONFLICT (id) DO NOTHING")
                    cursor.execute("INSERT INTO schema_info (id, version) VALUES (1, %s) ON CONFLICT (id) DO UPDATE SET version = EXCLUDED.version", (CURRENT_VERSION,))
            except Exception as e: logger.warning(f"Migration error on {name}: {e}")

    def _connect_providers(self):
        if self.nhost_enabled: self._reconnect_provider('A')
        if self.neon_enabled: self._reconnect_provider('B')
        if not self.provider_a_active and not self.provider_b_active: self._handle_total_failure()
        self._migrate_cloud_schema()

    def _handle_total_failure(self):
        logger.critical("Critical: No databases reachable.")
        sys.exit(1)

    def execute_query(self, sql, params=None, is_write=False, fetch_one=False, fetch_all=False):
        params = params or ()
        if is_write:
            for p_id, active, conn in [('A', self.provider_a_active, self.conn_a), ('B', self.provider_b_active, self.conn_b)]:
                if active:
                    try:
                        with conn.cursor() as cursor:
                            cursor.execute(sql, params)
                    except Exception as e:
                        logger.error(f"Write error: {e}")
                        if p_id == 'A': self.provider_a_active = False
                        else: self.provider_b_active = False
            return True
        else:
            options = [c for a, c in [(self.provider_a_active, self.conn_a), (self.provider_b_active, self.conn_b)] if a]
            if not options: return None
            conn = random.choice(options)
            try:
                with conn.cursor() as cursor:
                    cursor.execute(sql, params)
                    return cursor.fetchone() if fetch_one else cursor.fetchall() if fetch_all else None
            except Exception as e:
                logger.error(f"Read error: {e}")
                return None

    def insert_file(self, file_data: dict):
        keys = [k for k in file_data.keys() if k not in ['id', 'sl_no']]
        vals = [file_data[k] for k in keys]
        update_str = ", ".join([f'"{k}" = EXCLUDED."{k}"' for k in keys if k != 'file_hash'])
        cols_joined = ", ".join([f'"{k}"' for k in keys])
        placeholders = ", ".join(["%s"]*len(keys))
        sql = f'INSERT INTO media_library ({cols_joined}) VALUES ({placeholders}) ON CONFLICT (file_hash) DO UPDATE SET {update_str} RETURNING sl_no, {cols_joined}'
        
        row = None
        for active, conn in [(self.provider_a_active, self.conn_a), (self.provider_b_active, self.conn_b)]:
            if active:
                try:
                    with conn.cursor() as cursor:
                        cursor.execute(sql, tuple(vals))
                        row = cursor.fetchone()
                        break 
                except Exception as e: logger.error(f"Insert error: {e}")

        if row and self.cache_cursor:
            cols = ['sl_no'] + keys
            with self._sqlite_lock:
                self.cache_cursor.execute(f"INSERT OR REPLACE INTO media_library ({', '.join([f'\"{c}\"' for c in cols])}) VALUES ({', '.join(['?']*len(cols))})", row)
                self.cache_conn.commit()
        self.increment_storage_summary(file_data)

    def increment_storage_summary(self, file_data):
        self._stats_queue.put(("increment_storage_summary", (file_data.copy(),)))

    def _do_increment_storage_summary(self, file_data):
        try:
            filename = file_data.get("filename", "")
            size = file_data.get("file_size_bytes", 0)
            ext = os.path.splitext(filename or "")[1].lower()
            is_p = ext in {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp', '.heic', '.heif', '.tiff'}
            is_v = ext in {'.mp4', '.mov', '.avi', '.mkv', '.wmv', '.flv', '.webm'}
            gb = size / (1024**3)
            p_inc, v_inc = (1, 0) if is_p else (0, 1) if is_v else (0, 0)
            pg_inc, vg_inc = (gb, 0) if is_p else (0, gb) if is_v else (0, 0)

            sql = 'UPDATE storage_summary SET total_photos=total_photos+%s, total_videos=total_videos+%s, total_assets=total_assets+1, total_photos_size_gb=total_photos_size_gb+%s, total_videos_size_gb=total_videos_size_gb+%s, total_size_gb=total_size_gb+%s, updated_at=CURRENT_TIMESTAMP WHERE id=1'
            params = (p_inc, v_inc, pg_inc, vg_inc, gb)
            self.execute_query(sql, params, is_write=True)
            if self.cache_cursor:
                with self._sqlite_lock:
                    self.cache_cursor.execute(sql.replace('%s', '?'), params)
                    self.cache_conn.commit()
        except Exception as e: logger.error(f"Background stats error: {e}")

    def refresh_storage_summary(self, use_local_for_calc=True):
        self._stats_queue.put(("refresh_storage_summary", (use_local_for_calc,)))

    def _do_refresh_storage_summary(self, use_local_for_calc):
        logger.info("📊 Summary refresh.")

    def reconcile_databases(self):
        logger.info("🔍 Running Smart Reconciliation...")
        for table in ["media_library", "trips_config", "device_config"]:
            self._smart_sync_table(table)

    def _smart_sync_table(self, table):
        pk = {"media_library": "file_hash", "trips_config": "name", "device_config": "device_name"}[table]
        local_state = {}
        if self.cache_cursor:
            with self._sqlite_lock:
                rows = self.cache_cursor.execute(f'SELECT "{pk}", updated_at FROM "{table}"').fetchall()
                local_state = {r[0]: str(r[1]) for r in rows}
        cloud_state = {}
        rows = self.execute_query(f'SELECT "{pk}", updated_at FROM "{table}"', fetch_all=True)
        if rows: cloud_state = {r[0]: str(r[1]) for r in rows}

        to_push = [k for k, v in local_state.items() if k not in cloud_state or v > cloud_state.get(k, '')]
        to_pull = [k for k, v in cloud_state.items() if k not in local_state or v > local_state.get(k, '')]
        if not to_push and not to_pull: logger.info(f"✅ {table} in sync.")
        else: logger.info(f"🔄 {table}: {len(to_push)} push, {len(to_pull)} pull.")

    def init_local_cache(self):
        cache_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "Data", "local_cache.db")
        os.makedirs(os.path.dirname(cache_path), exist_ok=True)
        self.cache_conn = sqlite3.connect(cache_path, check_same_thread=False)
        self.cache_cursor = self.cache_conn.cursor()
        self.cache_conn.executescript('''
            CREATE TABLE IF NOT EXISTS media_library (file_hash TEXT PRIMARY KEY, sl_no INTEGER, filename TEXT, file_size_bytes INTEGER, upload_date TEXT, account_email TEXT, device_source TEXT, remote_id TEXT, album_name TEXT, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
            CREATE TABLE IF NOT EXISTS trips_config (name TEXT PRIMARY KEY, sl_no INTEGER, start TEXT, end TEXT, require_gps BOOLEAN, album_id TEXT, album_url TEXT, email_message_id TEXT, asset_metadata TEXT, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
            CREATE TABLE IF NOT EXISTS device_config (device_name TEXT PRIMARY KEY, directories TEXT, sl_no INTEGER, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
            CREATE TABLE IF NOT EXISTS sync_tracker (id INTEGER PRIMARY KEY, last_sync_time TIMESTAMP DEFAULT '1970-01-01 00:00:00');
            INSERT OR IGNORE INTO sync_tracker (id, last_sync_time) VALUES (1, '1970-01-01 00:00:00');
        ''')
        self.cache_conn.commit()
        logger.info(f"✅ Local Cache initialized.")

    def get_trips(self): return []
    def upsert_device_config_local(self, d, dirs):
        if self.cache_cursor:
            with self._sqlite_lock:
                self.cache_cursor.execute('INSERT OR REPLACE INTO device_config (device_name, directories) VALUES (?, ?)', (d, dirs))
                self.cache_conn.commit()

    def close(self):
        self._stats_queue.put(None)
        if self.cache_conn: self.cache_conn.close()
        if self.conn_a: self.conn_a.close()
        if self.conn_b: self.conn_b.close()
    def __enter__(self): return self
    def __exit__(self, *args): self.close()

if __name__ == "__main__":
    db = DatabaseBalancer(True)
    print("Balanced.")
