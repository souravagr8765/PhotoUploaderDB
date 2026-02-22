import os
import sqlite3
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional

app = FastAPI(title="PhotoUploaderDB - Immich UI")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "Data", "local_cache.db")
THUMBS_DIR = os.path.join(BASE_DIR, "Data", "Thumbnails")

# Ensure directories exist
os.makedirs(THUMBS_DIR, exist_ok=True)

class MediaItem(BaseModel):
    id: Optional[str]
    filename: Optional[str]
    album_name: Optional[str]
    upload_date: Optional[str]
    thumbid: Optional[str]
    file_size_bytes: Optional[int] = None
    account_email: Optional[str] = None
    device_source: Optional[str] = None

def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

# Ensure schema has thumbid column if user runs web UI before main script
def ensure_schema():
    if os.path.exists(DB_PATH):
        try:
            conn = get_db_connection()
            conn.execute("ALTER TABLE media_library ADD COLUMN thumbid TEXT")
            conn.commit()
            conn.close()
        except sqlite3.OperationalError:
            pass # Column already exists, all good!

ensure_schema()

@app.get("/api/media", response_model=List[MediaItem])
def get_media():
    if not os.path.exists(DB_PATH):
        return []
    
    conn = get_db_connection()
    cursor = conn.cursor()
    # If the user has a lot of media, we might want pagination.
    # For now, returning all to allow the frontend to group by date (timeline view)
    cursor.execute("""
        SELECT id, filename, album_name, upload_date, thumbid, file_size_bytes, account_email, device_source 
        FROM media_library 
        ORDER BY upload_date DESC
    """)
    rows = cursor.fetchall()
    conn.close()
    
    return [dict(row) for row in rows]

@app.get("/api/stats")
def get_stats():
    if not os.path.exists(DB_PATH):
        return {"total_files": 0, "total_size_mb": 0}
        
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*), SUM(file_size_bytes) FROM media_library")
    row = cursor.fetchone()
    conn.close()
    
    count = row[0] if row[0] else 0
    size_bytes = row[1] if row[1] else 0
    size_mb = size_bytes / (1024 * 1024)
    
    return {
        "total_files": count,
        "total_size_mb": round(size_mb, 2)
    }

@app.get("/api/thumbnails/{thumbid}")
def get_thumbnail(thumbid: str):
    # Security check: prevent directory traversal
    if ".." in thumbid or "/" in thumbid or "\\" in thumbid:
        raise HTTPException(status_code=400, detail="Invalid thumbid")
        
    path = os.path.join(THUMBS_DIR, f"{thumbid}.jpg")
    if os.path.exists(path):
         return FileResponse(path)
         
    # Return a generic placeholder or 404
    raise HTTPException(status_code=404, detail="Thumbnail not found")

@app.get("/")
def serve_index():
    index_path = os.path.join(BASE_DIR, "templates", "index.html")
    if os.path.exists(index_path):
        return FileResponse(index_path)
    return HTMLResponse("<h1>Welcome to PhotoUploader UI. index.html not found.</h1>")

# Mount static files if they exist (for CSS/JS)
STATIC_DIR = os.path.join(BASE_DIR, "static")
os.makedirs(STATIC_DIR, exist_ok=True)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

if __name__ == "__main__":
    import uvicorn
    # Run loop
    uvicorn.run(app, host="0.0.0.0", port=5000)
