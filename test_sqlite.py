import sqlite3
import time

conn = sqlite3.connect('Data/local_cache.db')
cursor = conn.cursor()

start = time.time()
cursor.execute("SELECT 1 FROM media_library WHERE LOWER(filename) = LOWER('test.jpg') LIMIT 1")
cursor.fetchone()
print(f"LOWER query took: {time.time() - start:.4f}s")

start = time.time()
cursor.execute("SELECT 1 FROM media_library WHERE filename = 'test.jpg' COLLATE NOCASE LIMIT 1")
cursor.fetchone()
print(f"COLLATE NOCASE query took: {time.time() - start:.4f}s")

# Let's count rows
cursor.execute("SELECT count(*) FROM media_library")
print(f"Total rows: {cursor.fetchone()[0]}")
