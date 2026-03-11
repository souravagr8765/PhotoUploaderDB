import os
import re
from datetime import datetime
import infra.logger as logger

# --- Dependency Check ---
try:
    from PIL import Image, ExifTags
    HAS_PIL = True
except ImportError:
    HAS_PIL = False
    logger.warning("⚠️ Pillow not found! Metadata extraction will be disabled.")

def get_photo_metadata(filepath):
    """
    Extracts basic metadata (DateTimeOriginal, HasGPS) from a photo, 
    or uses OS file modification time as fallback for videos/images without EXIF.
    Returns: (datetime_object, has_gps_bool) or (None, False) if failed.
    """
    date_taken = None
    has_gps = False
    
    # 1. Image EXIF check
    is_image = filepath.lower().endswith(('.jpg', '.jpeg', '.heic', '.png', '.webp', '.bmp', '.gif'))
    if HAS_PIL and is_image:
        try:
            with Image.open(filepath) as img:
                exif = img.getexif() if hasattr(img, 'getexif') else getattr(img, '_getexif', lambda: None)()
                if exif:
                    for key, val in exif.items():
                        tag_name = ExifTags.TAGS.get(key, key)
                        
                        if tag_name == "DateTimeOriginal":
                            try:
                                date_taken = datetime.strptime(str(val), "%Y:%m:%d %H:%M:%S")
                            except (ValueError, TypeError): pass
                        
                        if tag_name == "GPSInfo":
                            has_gps = True
        except Exception as e:
            logger.debug(f"Metadata error for {os.path.basename(filepath)}: {e}")

    # 2. Fallback for Videos (and images without parsed EXIF)
    if not date_taken:
        try:
            mtime = os.path.getmtime(filepath)
            date_taken = datetime.fromtimestamp(mtime)
        except Exception as e:
            logger.debug(f"Fallback metadata error for {os.path.basename(filepath)}: {e}")

    return date_taken, has_gps

def extract_date_from_file_fallback(filepath, date_taken):
    if date_taken:
        return date_taken
        
    filename = os.path.basename(filepath)
    filename_lower = filename.lower()
    
    # 1. WhatsApp
    if "wa" in filename_lower:
        match = re.search(r'(img|vid)-(\d{8})-wa\d+', filename_lower)
        if match:
            try:
                return datetime.strptime(match.group(2), "%Y%m%d")
            except ValueError:
                pass

    # 2. Screenshot
    if "screenshot" in filename_lower:
        match = re.search(r'screenshot_(\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2})', filename_lower)
        if match:
            try:
                return datetime.strptime(match.group(1), "%Y-%m-%d-%H-%M-%S")
            except ValueError:
                pass

    # 3. Regular Photo or Video
    match = re.search(r'(img|vid)(\d{14})', filename_lower)
    if match:
        try:
            return datetime.strptime(match.group(2), "%Y%m%d%H%M%S")
        except ValueError:
            pass

    return None
