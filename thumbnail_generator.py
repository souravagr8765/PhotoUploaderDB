import os
import logging
import subprocess
from PIL import Image

logger = logging.getLogger("ThumbnailGen")

THUMBS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "Data", "Thumbnails")
os.makedirs(THUMBS_DIR, exist_ok=True)

IMAGE_EXTS = ('.jpg', '.jpeg', '.png', '.gif', '.bmp')
VIDEO_EXTS = ('.mp4', '.mov', '.avi', '.mkv', '.webm')

def generate_thumbnail(filepath: str, thumbid: str) -> bool:
    """
    Generates a 256x256 max thumbnail for a given file and saves it as <thumbid>.jpg
    Supports common image formats and videos via ffmpeg.
    """
    thumb_path = os.path.join(THUMBS_DIR, f"{thumbid}.jpg")
    
    # If thumbnail already exists (for some reason), skip
    if os.path.exists(thumb_path):
        return True
        
    ext = os.path.splitext(filepath)[1].lower()
    
    try:
        if ext in IMAGE_EXTS:
            with Image.open(filepath) as img:
                img.thumbnail((256, 256))
                # Convert to RGB if necessary (e.g. for PNG with alpha)
                if img.mode != 'RGB':
                    img = img.convert('RGB')
                img.save(thumb_path, format='JPEG', quality=85)
            return True
            
        elif ext in VIDEO_EXTS:
            # Use ffmpeg to extract a frame at 1 second mark
            # Scale it to max 256 width or height while maintaining aspect ratio
            cmd = [
                'ffmpeg',
                '-y', # Overwrite
                '-i', filepath,
                '-ss', '00:00:01.000', # Seek to 1 second
                '-vframes', '1', # Extract 1 frame
                '-vf', 'scale=\'min(256,iw)\':\'min(256,ih)\':force_original_aspect_ratio=decrease',
                '-f', 'image2',
                thumb_path
            ]
            
            # Suppress output for clean logs
            result = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            return result.returncode == 0
            
        elif ext == '.heic':
            # HEIC is tricky. Termux usually can support HEIC via ImageMagick or standard ffmpeg
            # Let's try ffmpeg as a fallback for everything else
            cmd = [
                'ffmpeg',
                '-y',
                '-i', filepath,
                '-vf', 'scale=\'min(256,iw)\':\'min(256,ih)\':force_original_aspect_ratio=decrease',
                '-f', 'image2',
                thumb_path
            ]
            result = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            return result.returncode == 0
            
    except Exception as e:
        logger.error(f"Failed to generate thumbnail for {filepath}: {e}")
        return False
        
    return False
