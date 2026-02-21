import sys
import os
import json
import logging
import hashlib
from PIL import Image, ExifTags
from database import DatabaseManager

# Setup basic logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("Migrator")

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "Data")
ALL_INDICES_DIR = os.path.join(DATA_DIR, "all_indices")

# Placeholder - User must set this or pass as arg in future
MIGRATION_SOURCE_DIRS = [
    "E:/FamilyMemories/library/admin", 
]

def calculate_file_hash(filepath: str) -> str:
    """Calculates SHA-256 hash of a file."""
    sha256_hash = hashlib.sha256()
    try:
        with open(filepath, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()
    except Exception:
        return None


VALID_IMAGE_EXTS = ('.jpg', '.jpeg', '.heic', '.png', '.webp')
VALID_VIDEO_EXTS = ('.mp4', '.mov', '.avi', '.mkv', '.webm')

def get_exif_device_name(filepath: str):
    """Extracts Camera Model from EXIF (Images only)."""
    try:
        # Only try to open images with PIL
        if filepath.lower().endswith(VALID_IMAGE_EXTS):
            img = Image.open(filepath)
            exif = img._getexif()
            if not exif: return None
            
            for key, val in exif.items():
                if key in ExifTags.TAGS:
                    if ExifTags.TAGS[key] == 'Model':
                        return str(val).strip()
        
        # Videos: Currently no easy native metadata extraction without heavy deps.
        # Fallback to None (which defaults to legacy device name)
        elif filepath.lower().endswith(VALID_VIDEO_EXTS):
            return None
            
    except:
        return None
    return None

def find_file_in_sources(filename):
    """Searches for a filename in the source directories."""
    for folder in MIGRATION_SOURCE_DIRS:
        for root, _, files in os.walk(folder):
            if filename in files:
                return os.path.join(root, filename)
    return None

def generate_legacy_hash(filename):
    return hashlib.sha256(f"legacy_{filename.lower()}".encode()).hexdigest()


def stage_migration_data():
    logger.info("🚀 Starting Migration Staging (Phase 1)...")
    
    if not MIGRATION_SOURCE_DIRS:
        logger.warning("⚠️ No MIGRATION_SOURCE_DIRS defined! Real hashes cannot be computed.")
        logger.warning("   Please edit migrate.py to add your backup paths in line 16.")
    
    output_path = os.path.join(DATA_DIR, "migration_staged_data.json")
    staged_data = []
    processed_filenames = set()

    # 1. Load existing data to resume
    if os.path.exists(output_path):
        try:
            with open(output_path, 'r', encoding='utf-8') as f:
                staged_data = json.load(f)
                for item in staged_data:
                    processed_filenames.add(item['filename'])
            logger.info(f"🔄 Resuming... Found {len(staged_data)} processed records.")
        except Exception as e:
            logger.warning(f"⚠️ Could not read existing file, starting fresh: {e}")

    # 2. Iterate indices
    json_files = [f for f in os.listdir(ALL_INDICES_DIR) if f.endswith(".json")]
    
    data_changed = False
    items_since_save = 0
    SAVE_INTERVAL = 100

    for json_file in json_files:
        logger.info(f"📂 Processing Index: {json_file}")
        
        # Default device from filename
        default_device = json_file.replace("index_", "").replace(".json", "")
        
        path = os.path.join(ALL_INDICES_DIR, json_file)
        try:
            with open(path, 'r', encoding='utf-8') as f:
                data = json.load(f)

            for item in data:
                filename = item.get('filename')
                if not filename: continue
                
                # SKIP if already processed
                if filename in processed_filenames:
                    continue

                # Try to find real file
                real_path = find_file_in_sources(filename)
                
                if real_path:
                    # Found! Get real data
                    f_hash = calculate_file_hash(real_path)
                    device = get_exif_device_name(real_path) or default_device
                    source_status = "FOUND_LOCAL"
                else:
                    # Missing
                    f_hash = generate_legacy_hash(filename)
                    device = default_device
                    source_status = "MISSING_SOURCE"
                
                record = {
                    "file_hash": f_hash,
                    "filename": filename,
                    "file_size_bytes": item.get('size'),
                    "account_email": item.get('account'),
                    "device_source": device,
                    "remote_id": "legacy_import",
                    "migration_status": source_status
                }
                staged_data.append(record)
                processed_filenames.add(filename)
                
                items_since_save += 1
                data_changed = True
                
                # Periodic Save
                if items_since_save >= SAVE_INTERVAL:
                    logger.info(f"💾 Saving progress... ({len(staged_data)} total)")
                    with open(output_path, 'w', encoding='utf-8') as f:
                        json.dump(staged_data, f, indent=4)
                    items_since_save = 0
                    data_changed = False

        except Exception as e:
            logger.error(f"Error reading {json_file}: {e}")

    # Final Save
    if data_changed or not os.path.exists(output_path):
         with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(staged_data, f, indent=4)
        
    logger.info(f"✅ Staging Complete. {len(staged_data)} records saved to {output_path}")
    logger.info("Please review this file. When ready, tell me to 'commit' to database.")



from tqdm import tqdm

def commit_migration():
    logger.info("🚀 Starting Migration Commit (Phase 2)...")
    
    staged_path = os.path.join(DATA_DIR, "migration_staged_data.json")
    if not os.path.exists(staged_path):
        logger.error(f"❌ Staged data not found at {staged_path}")
        logger.error("   Run without --commit first to generate it.")
        return

    # 1. Connect DB
    try:
        db = DatabaseManager()
        if not db.check_connection():
            logger.error("Failed to connect to Supabase.")
            return
    except Exception as e:
        logger.error(f"DB Init Failed: {e}")
        return

    # 2. Load Staged Data
    with open(staged_path, 'r', encoding='utf-8') as f:
        staged_data = json.load(f)
    
    logger.info(f"📦 Loaded {len(staged_data)} records to commit.")
    
    total_committed = 0
    errors = 0
    skipped = 0
    
    # Progress Bar Loop
    for record in tqdm(staged_data, desc="Committing to Cloud", unit="file"):
        db_record = {
            "file_hash": record.get("file_hash"),
            "filename": record.get("filename"),
            "file_size_bytes": record.get("file_size_bytes"),
            "account_email": record.get("account_email"),
            "device_source": record.get("device_source"),
            "remote_id": record.get("remote_id")
        }
        
        # Check if already exists (Idempotency) using hash
        if db.file_exists_by_hash(db_record["file_hash"]):
            skipped += 1
            continue
            
        try:
            db.insert_file(db_record)
            total_committed += 1
        except Exception as e:
            # Don't break the progress bar with logs, just count
            errors += 1

    print("\n" + "="*30)
    logger.info(f"🎉 Migration Commit Complete.")
    logger.info(f"   Sent: {total_committed}")
    logger.info(f"   Skipped (Already Exists): {skipped}")
    logger.info(f"   Errors: {errors}")

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--commit":
        commit_migration()
    else:
        stage_migration_data()
