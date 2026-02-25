import os
import sqlite3
import logging
import random
import smtplib
from email.mime.text import MIMEText
from datetime import datetime
from dotenv import load_dotenv
import urllib.parse
import sys

import pg8000.dbapi
from tqdm import tqdm

# Load env variables
load_dotenv()

# Logger setup specific to database operations
logger = logging.getLogger("Database")
logger.setLevel(logging.INFO)

def send_notification_email(subject: str, body: str):
    """Sends a lightweight notification email using SMTP."""
    smtp_server = os.getenv("SMTP_SERVER", "smtp.gmail.com")
    smtp_port = os.getenv("SMTP_PORT", "587")
    smtp_user = os.getenv("SENDER_EMAIL")
    smtp_pass = os.getenv("APP_PASSWORD")
    
    if not all([smtp_server, smtp_user, smtp_pass]):
        logger.warning(f"Email credentials not fully configured. Skipping email: {subject}")
        return

    notify_email = os.getenv("RECEIVER_EMAIL", smtp_user)
    
    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = smtp_user
    msg['To'] = notify_email

    try:
        server = smtplib.SMTP(smtp_server, int(smtp_port))
        server.starttls()
        server.login(smtp_user, smtp_pass)
        server.send_message(msg)
        server.quit()
        logger.info(f"📧 Notification sent: {subject}")
    except Exception as e:
        logger.error(f"❌ Failed to send email '{subject}': {e}")


class DatabaseBalancer:
    def __init__(self, use_local_cache=False):
        # We assume standard PostgreSQL connection URIs in the environment
        self.nhost_url = os.getenv("NHOST_DB_URL")
        self.neon_url = os.getenv("NEON_DB_URL")
        
        if not self.nhost_url or not self.neon_url:
            logger.warning("Missing NHOST_DB_URL or NEON_DB_URL in .env. Attempting to run with missing DB providers.")
            
        self.provider_a_active = False
        self.provider_b_active = False
        
        self.conn_a = None
        self.conn_b = None
        
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

    def _connect_providers(self):
        if self.nhost_url:
            try:
                kwargs_a = self._parse_url(self.nhost_url)
                self.conn_a = pg8000.dbapi.connect(**kwargs_a)
                self.conn_a.autocommit = True
                self.provider_a_active = True
                logger.info("✅ Connected to Nhost (Provider A).")
            except Exception as e:
                self.provider_a_active = False
                logger.error(f"❌ Nhost Connection Failed: {e}")
                self._handle_single_failure("Nhost (A)", str(e))
                
        if self.neon_url:
            try:
                kwargs_b = self._parse_url(self.neon_url)
                self.conn_b = pg8000.dbapi.connect(**kwargs_b)
                self.conn_b.autocommit = True
                self.provider_b_active = True
                logger.info("✅ Connected to Neon (Provider B).")
            except Exception as e:
                self.provider_b_active = False
                logger.error(f"❌ Neon Connection Failed: {e}")
                self._handle_single_failure("Neon (B)", str(e))
                
        if not self.provider_a_active and not self.provider_b_active:
            self._handle_total_failure()
            
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
                try:
                    cursor_a = self.conn_a.cursor()
                    cursor_a.execute(sql, params)
                    success_a = True
                    if fetch_one: res_a = cursor_a.fetchone()
                    elif fetch_all: res_a = cursor_a.fetchall()
                except Exception as e:
                    logger.error(f"Provider A Write Failed: {e}")
                    self.provider_a_active = False
                    self._handle_single_failure("Nhost (A)", str(e))
                    
            if self.provider_b_active:
                try:
                    cursor_b = self.conn_b.cursor()
                    cursor_b.execute(sql, params)
                    success_b = True
                    if fetch_one: res_b = cursor_b.fetchone()
                    elif fetch_all: res_b = cursor_b.fetchall()
                except Exception as e:
                    logger.error(f"Provider B Write Failed: {e}")
                    self.provider_b_active = False
                    self._handle_single_failure("Neon (B)", str(e))
                    
            if not self.provider_a_active and not self.provider_b_active:
                self._handle_total_failure()
                
            if (self.provider_a_active and not success_a) or (self.provider_b_active and not success_b):
                raise Exception("Synchronous mirrored write failed on an active provider!")
                
            return res_a if success_a else res_b
            
        else:
            # Round-Robin / Random Read Select
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
        """Ensures the auto-increment sequences are up to date with the max sl_no."""
        for active, conn, name in [(self.provider_a_active, self.conn_a, "Nhost (A)"), 
                                   (self.provider_b_active, self.conn_b, "Neon (B)")]:
            if active:
                try:
                    cursor = conn.cursor()
                    cursor.execute("SELECT COALESCE(MAX(sl_no), 1) FROM media_library")
                    max_val = cursor.fetchone()[0]
                    
                    cursor.execute("SELECT pg_get_serial_sequence('media_library', 'sl_no')")
                    seq_res = cursor.fetchone()
                    seq_name = seq_res[0] if seq_res and seq_res[0] else 'media_library_sl_no_seq'
                    
                    cursor.execute("SELECT setval(%s, %s)", (seq_name, max_val))
                    logger.debug(f"Synced sequence on {name} to {max_val}")
                except Exception as e:
                    logger.warning(f"⚠️ Could not sync sequence on {name}: {e}")

    def reconcile_databases(self):
        """Self-Heal Phase: Reconciles databases using max(sl_no)."""
        logger.info("🔍 Running Initialization & Auto-Reconciliation...")
        if not self.provider_a_active or not self.provider_b_active:
            logger.info("One or both providers offline. Skipping full reconciliation.")
            self._sync_sequences()
            return
            
        max_a = 0
        max_b = 0
        
        try:
            # We must explicitly query each provider instead of relying on the execute_query random router
            cursor_a = self.conn_a.cursor()
            cursor_a.execute("SELECT MAX(sl_no) FROM media_library")
            res_a = cursor_a.fetchone()
            if res_a and res_a[0]: max_a = res_a[0]
        except Exception as e:
            logger.error(f"Failed to query Max SL_NO from A: {e}")
            
        try:
            cursor_b = self.conn_b.cursor()
            cursor_b.execute("SELECT MAX(sl_no) FROM media_library")
            res_b = cursor_b.fetchone()
            if res_b and res_b[0]: max_b = res_b[0]
        except Exception as e:
             logger.error(f"Failed to query Max SL_NO from B: {e}")
        
        if max_a == max_b:
            logger.info(f"✅ Both providers in sync (Max sl_no: {max_a}).")
            self._sync_sequences()
            return
            
        logger.warning(f"⚠️ Mismatch detected! Nhost(A): {max_a}, Neon(B): {max_b}")
        
        leading_conn = None
        lagging_conn = None
        lagging_name = ""
        leading_name = ""
        lagging_max = 0
        leading_max = 0
        
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
            
        logger.info(f"Leader is {leading_name}, Lagger is {lagging_name}. Fetching missing rows...")
        
        try:
            cursor_lead = leading_conn.cursor()
            cursor_lead.execute("SELECT * FROM media_library WHERE sl_no > %s ORDER BY sl_no ASC", (lagging_max,))
            missing_rows = cursor_lead.fetchall()
            
            if not missing_rows:
                return
                
            col_names = [desc[0] for desc in cursor_lead.description]
            cursor_lag = lagging_conn.cursor()
            placeholders = ', '.join(['%s'] * len(col_names))
            cols_str = ', '.join(col_names)
            
            insert_sql = f"INSERT INTO media_library ({cols_str}) VALUES ({placeholders})"
            
            for row in tqdm(missing_rows, desc=f"Syncing to {lagging_name}", unit="rows"):
                cursor_lag.execute(insert_sql, row)
                
            if self.cache_cursor:
                sqlite_placeholders = ', '.join(['?'] * len(col_names))
                sqlite_insert = f"REPLACE INTO media_library ({cols_str}) VALUES ({sqlite_placeholders})"
                for row in tqdm(missing_rows, desc="Syncing to local cache", unit="rows"):
                    self.cache_cursor.execute(sqlite_insert, row)
                self.cache_conn.commit()
                
            subject = "Recovery Successful"
            body = f"Reconciled databases.\nIdentified {lagging_name} as lagging by {len(missing_rows)} rows.\nSynced rows successfully to {lagging_name} and local cache."
            logger.info(f"✅ {body}")
            send_notification_email(subject, body)
            
        except Exception as e:
            logger.error(f"❌ Failed to reconcile databases: {e}")
            
        self._sync_sequences()

    def init_local_cache(self):
        """Initializes transient local SQLite connection."""
        try:
            cache_path = os.path.join(os.path.dirname(__file__), "Data", "local_cache.db")
            os.makedirs(os.path.dirname(cache_path), exist_ok=True)
            self.cache_conn = sqlite3.connect(cache_path)
            self.cache_cursor = self.cache_conn.cursor()
            
            self.cache_cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='media_library'")
            if not self.cache_cursor.fetchone():
                self.cache_cursor.execute("""
                    CREATE TABLE media_library (
                        sl_no INTEGER PRIMARY KEY,
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
            
            self.cache_cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='trips_config'")
            if not self.cache_cursor.fetchone():
                self.cache_cursor.execute("""
                    CREATE TABLE trips_config (
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
        """Downloads ALL data from Cloud to Local Cache."""
        if not self.cache_conn: return
        
        logger.info("🔄 Syncing Cloud DB to Local Cache...")
        try:
            max_sl_no = 0
            if self.cache_cursor:
                self.cache_cursor.execute("SELECT MAX(sl_no) FROM media_library")
                res = self.cache_cursor.fetchone()
                if res and res[0] is not None:
                    max_sl_no = res[0]
                    
            sql = "SELECT sl_no, file_hash, filename, file_size_bytes, upload_date, account_email, device_source, remote_id, album_name, thumbid FROM media_library WHERE sl_no > %s ORDER BY sl_no ASC"
            rows = self.execute_query(sql, (max_sl_no,), fetch_all=True)
            
            if rows:
                for row in rows:
                    self.cache_cursor.execute("""
                        REPLACE INTO media_library 
                        (sl_no, file_hash, filename, file_size_bytes, upload_date, account_email, device_source, remote_id, album_name, thumbid)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, row)
                self.cache_conn.commit()
                
            logger.info(f"✅ Sync Complete. {len(rows) if rows else 0} new records.")
            
            t_sql = "SELECT name, start, \"end\", require_gps, album_id FROM trips_config"
            trips = self.execute_query(t_sql, fetch_all=True)
            if trips:
                for row in trips:
                    self.cache_cursor.execute("""
                        REPLACE INTO trips_config (name, start, end, require_gps, album_id)
                        VALUES (?, ?, ?, ?, ?)
                    """, row)
                self.cache_conn.commit()
                
        except Exception as e:
            logger.error(f"❌ Sync failed: {e}")

    def file_exists_by_name(self, filename: str) -> bool:
        """Phase 1: Local Cache Check, Fallback to Cloud."""
        filenames_to_check = [filename]
        
        lower_name = filename.lower()
        if lower_name.endswith('.jpg'):
            filenames_to_check.extend([filename[:-4] + '.jpeg', filename[:-4] + '.JPEG'])
        elif lower_name.endswith('.jpeg'):
            filenames_to_check.extend([filename[:-5] + '.jpg', filename[:-5] + '.JPG'])

        if self.cache_cursor:
            try:
                for fname in filenames_to_check:
                    self.cache_cursor.execute("SELECT 1 FROM media_library WHERE LOWER(filename) = LOWER(?) LIMIT 1", (fname,))
                    if self.cache_cursor.fetchone() is not None:
                        return True
            except: pass
                
        for fname in filenames_to_check:
            sql = "SELECT 1 FROM media_library WHERE LOWER(filename) = LOWER(%s) LIMIT 1"
            res = self.execute_query(sql, (fname,), fetch_one=True)
            if res: return True
        return False

    def file_exists_by_hash(self, file_hash: str) -> bool:
        """Phase 2: Local Cache Check, then Cloud."""
        if self.cache_cursor:
            try:
                self.cache_cursor.execute("SELECT 1 FROM media_library WHERE file_hash = ? LIMIT 1", (file_hash,))
                if self.cache_cursor.fetchone() is not None:
                    return True
            except: pass
                
        sql = "SELECT 1 FROM media_library WHERE file_hash = %s LIMIT 1"
        res = self.execute_query(sql, (file_hash,), fetch_one=True)
        if res: return True
        return False

    def insert_file(self, file_data: dict):
        """Inserts a new file record."""
        keys = []
        vals = []
        for k, v in file_data.items():
            if k not in ['id', 'sl_no']:
                keys.append(k)
                vals.append(v)
                
        cols_str = ', '.join(keys)
        placeholders = ', '.join(['%s'] * len(keys))
        
        sql = f"INSERT INTO media_library ({cols_str}) VALUES ({placeholders}) RETURNING sl_no, {cols_str}"
        row = self.execute_query(sql, tuple(vals), is_write=True, fetch_one=True)
        
        if self.cache_cursor and row:
            returned_keys = ['sl_no'] + keys
            sqlite_placeholders = ', '.join(['?'] * len(returned_keys))
            sqlite_cols = ', '.join(returned_keys)
            sqlite_insert = f"REPLACE INTO media_library ({sqlite_cols}) VALUES ({sqlite_placeholders})"
            self.cache_cursor.execute(sqlite_insert, row)
            self.cache_conn.commit()

    def get_trips(self):
        """Fetches all active trips."""
        if self.cache_cursor:
            try:
                self.cache_cursor.execute("SELECT name, start, end, require_gps, album_id FROM trips_config")
                rows = self.cache_cursor.fetchall()
                if rows:
                    return [{"name": r[0], "start": r[1], "end": r[2], "require_gps": bool(r[3]), "album_id": r[4]} for r in rows]
            except Exception as e:
                logger.error(f"❌ Failed to fetch trips locally: {e}")
                
        sql = "SELECT name, start, \"end\", require_gps, album_id FROM trips_config"
        rows = self.execute_query(sql, fetch_all=True)
        if not rows: return []
        return [{"name": r[0], "start": r[1], "end": r[2], "require_gps": bool(r[3]), "album_id": r[4]} for r in rows]

    def update_trip_album_id(self, trip_name: str, album_id: str):
        """Updates the album ID for a specific trip in both Cloud and Local Cache."""
        try:
            sql = "UPDATE trips_config SET album_id = %s WHERE name = %s"
            self.execute_query(sql, (album_id, trip_name), is_write=True)
            
            if self.cache_cursor:
                self.cache_cursor.execute("UPDATE trips_config SET album_id = ? WHERE name = ?", (album_id, trip_name))
                self.cache_conn.commit()
                logger.info(f"💾 Updated Album ID for trip '{trip_name}' in cache & cloud.")
        except Exception as e:
            logger.error(f"❌ Failed to update trip album ID: {e}")

    def backup_to_local_sqlite(self, backup_path: str):
        """Creates a snapshot backup of the local cache database."""
        if not self.cache_conn:
            logger.warning("No local cache to backup. Skipping.")
            return

        try:
            os.makedirs(os.path.dirname(backup_path), exist_ok=True)
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
