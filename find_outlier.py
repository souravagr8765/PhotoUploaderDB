
import sqlite3
import os

db_path = "Data/local_cache.db"
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

photo_extensions = ('jpg', 'jpeg', 'png', 'gif', 'bmp', 'webp', 'heic', 'heif', 'tiff')
video_extensions = ('mp4', 'mov', 'avi', 'mkv', 'wmv', 'flv', 'webm')

sql = """
SELECT filename FROM media_library 
WHERE NOT (
    filename LIKE '%.jpg' OR filename LIKE '%.jpeg' OR filename LIKE '%.png' OR filename LIKE '%.gif' OR filename LIKE '%.bmp' OR filename LIKE '%.webp' OR filename LIKE '%.heic' OR filename LIKE '%.heif' OR filename LIKE '%.tiff'
    OR filename LIKE '%.mp4' OR filename LIKE '%.mov' OR filename LIKE '%.avi' OR filename LIKE '%.mkv' OR filename LIKE '%.wmv' OR filename LIKE '%.flv' OR filename LIKE '%.webm'
);
"""

cursor.execute(sql)
rows = cursor.fetchall()
print(f"Found {len(rows)} rows that are neither photos nor videos:")
for row in rows:
    print(row[0])

conn.close()
