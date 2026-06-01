
import sqlite3
import os

db_path = "Data/local_cache.db"
if not os.path.exists(db_path):
    print(f"Database file not found at {db_path}")
    exit(1)

conn = sqlite3.connect(db_path)
cursor = conn.cursor()

try:
    cursor.execute("SELECT COUNT(*) FROM media_library;")
    media_library_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT total_assets FROM storage_summary WHERE id=1;")
    storage_summary_total = cursor.fetchone()[0]
    
    print(f"Total rows in media_library: {media_library_count}")
    print(f"Total assets in storage_summary: {storage_summary_total}")
    print(f"Difference: {media_library_count - storage_summary_total}")
    
    # Let's also check photo and video counts
    cursor.execute("""
        SELECT 
            SUM(CASE WHEN filename LIKE '%.jpg' OR filename LIKE '%.jpeg' OR filename LIKE '%.png' OR filename LIKE '%.gif' OR filename LIKE '%.bmp' OR filename LIKE '%.webp' OR filename LIKE '%.heic' OR filename LIKE '%.heif' OR filename LIKE '%.tiff' THEN 1 ELSE 0 END) as photos,
            SUM(CASE WHEN filename LIKE '%.mp4' OR filename LIKE '%.mov' OR filename LIKE '%.avi' OR filename LIKE '%.mkv' OR filename LIKE '%.wmv' OR filename LIKE '%.flv' OR filename LIKE '%.webm' THEN 1 ELSE 0 END) as videos
        FROM media_library;
    """)
    media_counts = cursor.fetchone()
    
    cursor.execute("SELECT total_photos, total_videos FROM storage_summary WHERE id=1;")
    summary_counts = cursor.fetchone()
    
    print(f"Media Library - Photos: {media_counts[0]}, Videos: {media_counts[1]}")
    print(f"Storage Summary - Photos: {summary_counts[0]}, Videos: {summary_counts[1]}")

except Exception as e:
    print(f"Error: {e}")
finally:
    conn.close()
