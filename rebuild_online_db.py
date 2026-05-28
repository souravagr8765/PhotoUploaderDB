import os
import sys
import sqlite3
import time
from tqdm import tqdm

# Ensure we can import from the project root
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.append(project_root)

from db.balancer import DatabaseBalancer

# Batch size of 1000 is usually a sweet spot for memory vs speed.
# Max params in PG is ~65k, so 1000 rows * 10-15 cols = 15k params (safe).
BATCH_SIZE = 1000

def ensure_connections(db):
    """Checks and attempts to restore connections to both providers if enabled."""
    if db.nhost_enabled and not db.provider_a_active:
        db._reconnect_provider('A')
    
    if db.neon_enabled and not db.provider_b_active:
        db._reconnect_provider('B')

def batch_insert(db, table, col_names, rows):
    """
    Performs high-speed multi-row INSERT into all active cloud providers.
    Uses 'INSERT INTO table (cols) VALUES (...), (...)' syntax for 10-50x speedup over standard executemany.
    """
    if not rows:
        return
        
    cols_str = ', '.join([f'"{c}"' if c in ["end", "order", "start"] else c for c in col_names])
    
    # Construct the multi-row INSERT statement
    # placeholders_per_row like "(%s, %s, %s)"
    placeholders_per_row = "(" + ", ".join(["%s"] * len(col_names)) + ")"
    # all_placeholders like "(%s, %s), (%s, %s), ..."
    all_placeholders = ", ".join([placeholders_per_row] * len(rows))
    
    insert_sql = f"INSERT INTO {table} ({cols_str}) VALUES {all_placeholders}"
    
    # Flatten all parameters into a single list
    flat_params = []
    for row in rows:
        for c in col_names:
            val = row[c]
            # Handle specific type conversions for PostgreSQL
            if table == "trips_config" and c == "require_gps":
                val = bool(val)
            flat_params.append(val)

    providers = []
    if db.provider_a_active: providers.append(('A', db.conn_a))
    if db.provider_b_active: providers.append(('B', db.conn_b))
    
    for name, conn in providers:
        success = False
        retry_count = 0
        while not success and retry_count < 3:
            try:
                cursor = conn.cursor()
                # Single execution of the large multi-row statement
                cursor.execute(insert_sql, flat_params)
                conn.commit()
                success = True
            except Exception as e:
                retry_count += 1
                # print(f"\n❌ Multi-row insert failed on Provider {name} (Attempt {retry_count}): {e}")
                time.sleep(1)
                # Try to reconnect using the balancer's logic
                if name == 'A':
                    db._reconnect_provider('A')
                    conn = db.conn_a
                else:
                    db._reconnect_provider('B')
                    conn = db.conn_b
        
        if not success:
             print(f"\n⚠️ Critical: Failed to complete batch insert on Provider {name} after retries.")

def main():
    print("🚀 Starting High-Speed Database Rebuild Script")
    
    # 1. Connect to local cache
    local_db_path = os.path.join(project_root, "Data", "local_cache.db")
    if not os.path.exists(local_db_path):
        print(f"❌ Local cache not found at {local_db_path}. Aborting.")
        return
        
    local_conn = sqlite3.connect(local_db_path)
    local_conn.row_factory = sqlite3.Row
    local_cursor = local_conn.cursor()

    # 2. Connect to cloud databases
    print("⏳ Connecting to Cloud Databases...")
    db = DatabaseBalancer(use_local_cache=False)
    
    # Tables in dependency order
    tables = [
        "storage_summary",
        "account_distribution",
        "device_distribution",
        "media_library",
        "trips_config",
        "device_config"
    ]
    
    # 3. Truncate tables on cloud
    print("\n🗑️ Truncating cloud tables...")
    ensure_connections(db)
    
    truncate_sql = f"TRUNCATE TABLE {', '.join(tables)} RESTART IDENTITY CASCADE;"
    for name, conn, active in [('A', db.conn_a, db.provider_a_active), ('B', db.conn_b, db.provider_b_active)]:
        if active:
            try:
                cursor = conn.cursor()
                cursor.execute(truncate_sql)
                conn.commit()
                print(f"  ✅ Truncated tables on Provider {name}")
            except Exception as e:
                print(f"  ❌ Failed to truncate on Provider {name}: {e}")

    # 4. Migrate data with multi-row batching and progress bars
    print("\n⬆️ Uploading data from local cache to cloud...")
    
    for table in tables:
        try:
            local_cursor.execute(f"SELECT COUNT(*) FROM {table}")
            total_rows = local_cursor.fetchone()[0]
        except sqlite3.OperationalError:
            continue
            
        if total_rows == 0:
            continue

        # Get column names from sample
        local_cursor.execute(f"SELECT * FROM {table} LIMIT 1")
        sample_row = local_cursor.fetchone()
        if not sample_row:
            continue
        col_names = list(sample_row.keys())
        
        # Query all data
        local_cursor.execute(f"SELECT * FROM {table}")
        
        with tqdm(total=total_rows, desc=f"Migrating {table:20}", unit="row") as pbar:
            while True:
                ensure_connections(db)
                batch = local_cursor.fetchmany(BATCH_SIZE)
                if not batch:
                    break
                
                batch_insert(db, table, col_names, batch)
                pbar.update(len(batch))
        
    print("\n🔄 Syncing sequences...")
    ensure_connections(db)
    db._sync_sequences()
    print("🎉 Rebuild Complete!")

if __name__ == "__main__":
    main()
