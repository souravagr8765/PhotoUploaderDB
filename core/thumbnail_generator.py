import os
import subprocess
import shutil
from PIL import Image
import infra.logger as logger

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
THUMBS_DIR = os.path.join(BASE_DIR, "Data", "Thumbnails")
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
            if shutil.which('ffmpeg') is None:
                logger.error(f"ffmpeg is not installed! Please install it (e.g., 'pkg install ffmpeg' in Termux) to generate video thumbnails for {filepath}.")
                return False

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
            subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, shell=False, check=True)
            return True
            
        elif ext == '.heic':
            # HEIC is tricky. Termux usually can support HEIC via ImageMagick or standard ffmpeg
            # Let's try ffmpeg as a fallback for everything else
            if shutil.which('ffmpeg') is None:
                logger.error(f"ffmpeg is not installed! Cannot generate thumbnail for HEIC image {filepath}.")
                return False

            cmd = [
                'ffmpeg',
                '-y',
                '-i', filepath,
                '-vf', 'scale=\'min(256,iw)\':\'min(256,ih)\':force_original_aspect_ratio=decrease',
                '-f', 'image2',
                thumb_path
            ]
            subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, shell=False, check=True)
            return True
            
    except Exception as e:
        logger.error(f"Failed to generate thumbnail for {filepath}: {e}")
        return False
        
    return False

def thumbnail_worker(in_queue):
    """
    Consumer for the thumbnail queue. Runs generation in the background
    so tracker thread is not blocked.
    """
    logger.info("🖼️ Thumbnail Thread: Started.")
    while True:
        item = in_queue.get()
        if item is None:
            in_queue.task_done()
            logger.info("🖼️ Thumbnail Thread: Received termination signal. Exiting.")
            break
            
        filepath = item.get("filepath")
        thumbid = item.get("thumbid")
        filename = item.get("filename")
        
        if filepath and thumbid:
            logger.debug(f"Generating thumbnail for {filename} ({thumbid})")
            success = generate_thumbnail(filepath, thumbid)
            if not success:
                logger.warning(f"⚠️ Background thumbnail generation failed for {filename}.")
        
        in_queue.task_done()

