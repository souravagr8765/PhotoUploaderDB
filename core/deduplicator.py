import os
import hashlib
from datetime import datetime
import infra.logger as logger
from metadata.extractor import get_photo_metadata, extract_date_from_file_fallback

def calculate_file_hash(filepath: str) -> str:
    """Calculates SHA-256 hash of a file, optimized for large files."""
    sha256_hash = hashlib.sha256()
    with open(filepath, "rb") as f:
        # Read in 1MB chunks to dramatically speed up Python loop overhead for large files
        for byte_block in iter(lambda: f.read(1048576), b""):
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
            # It's a renamed duplicate. Skip upload, but log as an alias.
            new_file_data = dict(original_file_data)
            new_file_data["filename"] = file
            
            # Estimate capture date for the alias record
            date_taken, _ = get_photo_metadata(filepath)
            date_taken = extract_date_from_file_fallback(filepath, date_taken)
            upload_date_str = date_taken.isoformat() if date_taken else datetime.now().isoformat()
            new_file_data["upload_date"] = upload_date_str
            
            if not dry_run:
                db.insert_file(new_file_data)
                logger.info(f"Added alias to DB: {file}")
            else:
                logger.info(f"🏜️ [DRY RUN] Would add alias to DB: {file}")
                
            local_filename_cache.add(file.lower())
            append_to_filename_cache(file)
            in_queue.task_done()
            continue

        # If it reaches here, it is a brand NEW file.
        item["hash"] = f_hash
        result_list.append(item)
        in_queue.task_done()
