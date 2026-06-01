import os
import hashlib
from datetime import datetime
import infra.logger as logger
from metadata.extractor import get_photo_metadata, extract_date_from_file_fallback

def calculate_file_hash(filepath: str) -> str:
    """Calculates SHA-256 hash. Uses sampled hashing for large files (>20MB) to drastically improve speed."""
    sha256_hash = hashlib.sha256()
    file_size = os.path.getsize(filepath)
    
    LARGE_FILE_THRESHOLD = 20 * 1024 * 1024  # 20 MB
    CHUNK_SIZE = 1048576  # 1 MB

    if file_size > LARGE_FILE_THRESHOLD:
        # Sampled Hashing: Inject file size into hash state to prevent collisions between different sized files
        sha256_hash.update(f"SAMPLED_{file_size}".encode('utf-8'))
        
        with open(filepath, "rb") as f:
            # 1. First 1MB
            sha256_hash.update(f.read(CHUNK_SIZE))
            
            # 2. Middle 1MB
            f.seek(file_size // 2)
            sha256_hash.update(f.read(CHUNK_SIZE))
            
            # 3. Last 1MB
            if file_size >= CHUNK_SIZE:
                f.seek(file_size - CHUNK_SIZE)
                sha256_hash.update(f.read(CHUNK_SIZE))
    else:
        # Full Hashing for smaller files
        with open(filepath, "rb") as f:
            for byte_block in iter(lambda: f.read(CHUNK_SIZE), b""):
                sha256_hash.update(byte_block)
                
    return sha256_hash.hexdigest()

def deduplicator_worker(in_queue, result_list: list, db, local_filename_cache, append_to_filename_cache, dry_run=False):
    """
    Consumer of scanner queue. Checks database for redundancy.
    Appends completely new (unuploaded) files to result_list for sequential processing.
    """
    logger.info("🔍 Deduplicator Thread: Started.")
    
    while True:
        item = in_queue.get()
        if item is None:
            in_queue.task_done()
            logger.info("🔍 Deduplicator Thread: Received termination signal. Exiting.")
            break
            
        file = item["filename"]
        filepath = item["filepath"]
        
        # --- PHASE 0: In-Memory Fast Cache Check ---
        if file.lower() in local_filename_cache:
            in_queue.task_done()
            continue # SKIP entirely without DB or logging to save time
            
        logger.info(f"Checking for the file in DB: {file}")
        
        # --- PHASE 1: Filename Check (Fast) ---
        if db.file_exists_by_name(file):
            logger.info(f"File already exists in DB(By Name): {file}")
            local_filename_cache.add(file.lower())
            append_to_filename_cache(file)
            in_queue.task_done()
            continue 
            
        # --- PHASE 2: Hash Check (Deep) ---
        logger.info(f"filename not found in the Database,Calculating hash for the file: {file}")
        f_hash = calculate_file_hash(filepath)
        original_file_data = db.get_file_by_hash(f_hash)
        
        if original_file_data:
            logger.info(f"File already exists in DB(by HASH): {file}")
            # It's a duplicate. We simply discard it to maintain a 1:1 hash-to-record ratio.
            if not dry_run:
                logger.info(f"Discarded redundant duplicate record: {file}")
            else:
                logger.info(f"🏜️ [DRY RUN] Would discard redundant duplicate record: {file}")
                
            local_filename_cache.add(file.lower())
            append_to_filename_cache(file)
            in_queue.task_done()
            continue

        # If it reaches here, it is a brand NEW file.
        item["hash"] = f_hash
        result_list.append(item)
        in_queue.task_done()
