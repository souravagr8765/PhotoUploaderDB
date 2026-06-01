import sqlite3
import os

db_path = os.path.join("Data", "local_cache.db")
if os.path.exists(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT directories FROM device_config WHERE device_name = 'Laptop_15s'")
    row = cursor.fetchone()
    if row:
        print(f"Directories: {row[0]}")
    else:
        print("No directories found for Laptop_15s")
    conn.close()
else:
    print("Database not found")
