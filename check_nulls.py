
import sqlite3
import os

db_path = "Data/local_cache.db"
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

cursor.execute("SELECT COUNT(*) FROM media_library WHERE account_email IS NULL;")
null_acc = cursor.fetchone()[0]

cursor.execute("SELECT COUNT(*) FROM media_library WHERE device_source IS NULL;")
null_dev = cursor.fetchone()[0]

cursor.execute("SELECT COUNT(*) FROM media_library WHERE album_name IS NULL;")
null_album = cursor.fetchone()[0]

print(f"Rows with NULL account_email: {null_acc}")
print(f"Rows with NULL device_source: {null_dev}")
print(f"Rows with NULL album_name: {null_album}")

conn.close()
