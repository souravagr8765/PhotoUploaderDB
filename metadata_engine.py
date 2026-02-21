import os
import logging
from datetime import datetime

# Configure Logging
logger = logging.getLogger("MetadataEngine")

# --- Dependency Check ---
try:
    from PIL import Image, ExifTags
    HAS_PIL = True
except ImportError:
    HAS_PIL = False
    logger.warning("⚠️ Pillow not found! Metadata extraction will be disabled.")

# Removed hardcoded JSON configuration logic

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
                exif = img.getexif() if hasattr(img, 'getexif') else img._getexif()
                if exif:
                    for key, val in exif.items():
                        tag_name = ExifTags.TAGS.get(key, key)
                        
                        if tag_name == "DateTimeOriginal":
                            try:
                                date_taken = datetime.strptime(str(val), "%Y:%m:%d %H:%M:%S")
                            except: pass
                        
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

def get_assigned_album(filepath, active_trips):
    """
    Determines if a photo belongs to a configured trip based on metadata.
    Returns: Trip dictionary or None.
    """
    date_obj, has_gps = get_photo_metadata(filepath)
    if not date_obj: return None
    
    date_str = date_obj.strftime("%Y-%m-%d") # Compare just dates
    is_video = not filepath.lower().endswith(('.jpg', '.jpeg', '.heic', '.png', '.webp', '.bmp', '.gif'))
    
    for trip in active_trips:
        # Check Date Range
        if trip["start"] <= date_str <= trip["end"]:
            # Check GPS Constraint (ignore for videos)
            if not is_video and trip.get("require_gps", False) and not has_gps:
                continue
                
            logger.info(f"🎯 Matched Album: {trip['name']} for {os.path.basename(filepath)}")
            return trip
            
    return None



