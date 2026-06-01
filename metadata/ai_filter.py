import os
import time
import subprocess
import requests
import json
import base64
from metadata.extractor import get_photo_metadata
import infra.logger as logger
from infra.config_loader import get_config

OLLAMA_HOST = get_config("ai_filtering.ollama_host", "http://localhost:11434")
MODEL_NAME = get_config("ai_filtering.model_name", "moondream")

def start_ollama():
    """Starts the Ollama server for the filtering session (if local)."""
    is_local = "localhost" in OLLAMA_HOST or "127.0.0.1" in OLLAMA_HOST
    
    if is_local:
        try:
            logger.info("🦙 Starting local Ollama service...")
            process = subprocess.Popen(["ollama", "serve"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except Exception as e:
            logger.error(f"❌ Failed to start local Ollama: {e}")
            return None
    else:
        logger.info(f"🌐 Using remote Ollama service at {OLLAMA_HOST}")
        process = "remote" # Sentinel

    # Wait for the server to respond to heartbeat
    ready = False
    for i in range(20):
        try:
            resp = requests.get(f"{OLLAMA_HOST}/api/tags", timeout=2)
            if resp.status_code == 200:
                logger.info(f"🦙 Ollama is ready at {OLLAMA_HOST}")
                ready = True
                break
        except:
            time.sleep(1)
    
    if not ready:
        logger.error(f"❌ Ollama server ({OLLAMA_HOST}) failed to respond within timeout.")
        if is_local and process != "remote": 
            process.terminate()
        return None
        
    return process

def stop_ollama(process):
    """Safely stops the Ollama server (if local)."""
    if process and process != "remote":
        try:
            logger.info("🦙 Stopping Ollama service...")
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
            logger.info("🦙 Ollama stopped successfully.")
        except Exception as e:
            logger.error(f"⚠️ Error stopping Ollama process: {e}")
    elif process == "remote":
        logger.info("🌐 Session finished for remote Ollama.")

def extract_video_frame(filepath):
    """Extracts the first frame of a video using ffmpeg for AI evaluation."""
    temp_frame = os.path.join(os.path.dirname(filepath), f"ai_tmp_{os.path.basename(filepath)}.jpg")
    try:
        # Extract first frame at original scale, high quality
        subprocess.run(
            ["ffmpeg", "-y", "-i", filepath, "-frames:v", "1", "-q:v", "2", temp_frame],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=True
        )
        if os.path.exists(temp_frame):
            return temp_frame
    except Exception as e:
        logger.debug(f"Video frame extraction failed for {filepath}: {e}")
    return None

def encode_image(image_path):
    """Encodes an image file to base64 for Ollama API."""
    with open(image_path, "rb") as img_file:
        return base64.b64encode(img_file.read()).decode('utf-8')

def evaluate_with_ai(filepath, trip_context):
    """
    Sends the media to local Ollama (moondream) for trip inclusion decision.
    Returns: 'include', 'exclude', or 'unsure'.
    """
    is_video = not filepath.lower().endswith(('.jpg', '.jpeg', '.heic', '.png', '.webp', '.bmp', '.gif'))
    image_to_send = filepath
    temp_frame = None
    
    if is_video:
        temp_frame = extract_video_frame(filepath)
        if temp_frame:
            image_to_send = temp_frame
        else:
            # If frame extraction fails, we can't send an image, so we return unsure
            return 'unsure'

    # Gather additional metadata for context
    date_taken, has_gps, lat, lon, width, height, has_exif = get_photo_metadata(filepath)
    
    # Construct prompt with context
    prompt = (
        f"I am organizing media for a trip titled '{trip_context['name']}'.\n"
        f"Trip Location: {trip_context.get('lat')}, {trip_context.get('lon')}\n"
        f"Trip Dates: {trip_context['start']} to {trip_context['end']}\n\n"
        "Task: Describe this image/video frame and decide if it is relevant to the trip context described above. "
        "Look for visual cues that connect the content to the trip's name, location, or the activity of traveling. "
        "If it belongs to the trip, include the word 'include' in your response. "
        "If it is irrelevant (e.g., a generic screenshot, an unrelated meme, or clearly from a different setting), include the word 'exclude'. "
        "If you are not sure, just describe what you see."
    )

    try:
        b64_image = encode_image(image_to_send)
        payload = {
            "model": MODEL_NAME,
            "prompt": prompt,
            "stream": False,
            "images": [b64_image]
        }
        
        resp = requests.post(f"{OLLAMA_HOST}/api/generate", json=payload, timeout=90)
        if resp.status_code == 200:
            ai_resp = resp.json().get("response", "").strip().lower()
            logger.debug(f"AI Response for {os.path.basename(filepath)}: {ai_resp}")
            
            # Robust Keyword Mapping (Trip-Agnostic)
            include_keywords = ['include', 'belongs', 'captured during', 'is relevant']
            exclude_keywords = ['exclude', 'irrelevant', 'not related', 'not relevant', 'no', 'away', 'generic', 'unrelated']
            
            # Check EXCLUDE first to avoid false positives from negations (e.g. "is not relevant")
            if any(k in ai_resp for k in exclude_keywords): 
                return 'exclude'
            if any(k in ai_resp for k in include_keywords): 
                return 'include'
            return 'unsure'
        else:
            logger.warning(f"⚠️ AI API error ({resp.status_code}): {resp.text}")
            return 'unsure'
    except Exception as e:
        logger.error(f"❌ AI evaluation failed for {filepath}: {e}")
        return 'unsure'
    finally:
        # Cleanup temp video frame
        if temp_frame and os.path.exists(temp_frame):
            try:
                os.remove(temp_frame)
            except: pass
