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
    Extracts basic metadata (DateTimeOriginal, HasGPS, GPSCoords, Resolution) from a photo.
    Returns: (date_taken, has_gps, lat, lon, width, height, has_exif)
    """
    date_taken = None
    has_gps = False
    lat, lon = None, None
    width, height = None, None
    has_exif = False
    
    # 1. Image EXIF check
    is_image = filepath.lower().endswith(('.jpg', '.jpeg', '.heic', '.png', '.webp', '.bmp', '.gif'))
    if HAS_PIL and is_image:
        try:
            with Image.open(filepath) as img:
                width, height = img.size
                exif = img.getexif() if hasattr(img, 'getexif') else getattr(img, '_getexif', lambda: None)()
                if exif:
                    has_exif = True
                    for key, val in exif.items():
                        tag_name = ExifTags.TAGS.get(key, key)
                        
                        if tag_name == "DateTimeOriginal":
                            try:
                                date_taken = datetime.strptime(str(val), "%Y:%m:%d %H:%M:%S")
                            except (ValueError, TypeError): pass
                        
                        if tag_name == "GPSInfo":
                            has_gps = True
                            lat, lon = get_gps_decimal(val)
        except Exception as e:
            logger.debug(f"Metadata error for {os.path.basename(filepath)}: {e}")

    # 2. Fallback for Videos (and images without parsed EXIF)
    if not date_taken:
        try:
            mtime = os.path.getmtime(filepath)
            date_taken = datetime.fromtimestamp(mtime)
        except Exception as e:
            logger.debug(f"Fallback metadata error for {os.path.basename(filepath)}: {e}")

    return date_taken, has_gps, lat, lon, width, height, has_exif

def get_gps_decimal(gps_info):
    """Converts EXIF GPSInfo to decimal degrees."""
    def convert_to_degrees(value):
        d = float(value[0])
        m = float(value[1])
        s = float(value[2])
        return d + (m / 60.0) + (s / 3600.0)

    try:
        # GPSInfo mapping: 1=N/S, 2=Lat, 3=E/W, 4=Lon
        lat = convert_to_degrees(gps_info[2])
        if gps_info[1] != 'N': lat = 0 - lat
        
        lon = convert_to_degrees(gps_info[4])
        if gps_info[3] != 'E': lon = 0 - lon
        
        return lat, lon
    except Exception:
        return None, None

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
