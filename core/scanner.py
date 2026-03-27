import os
import infra.logger as logger

# --- FILE EXTENSIONS ---
VALID_EXTENSIONS = ('.jpg', '.jpeg', '.png', '.gif', '.bmp',
                    '.mp4', '.mov', '.avi', '.mkv', '.webm')

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
IGNORE_FILE_PATH = os.path.join(BASE_DIR, "Data", ".ignore")


def load_ignore_set() -> set:
    """
    Loads the ignore list from Data/.ignore.
    Each line is a filename to ignore (case-insensitive). Lines starting with '#' are comments.
    """
    ignore_set = set()
    if not os.path.exists(IGNORE_FILE_PATH):
        return ignore_set

    with open(IGNORE_FILE_PATH, "r", encoding="utf-8") as f:
        for line in f:
            stripped = line.strip()
            if stripped and not stripped.startswith("#"):
                ignore_set.add(stripped.lower())

    logger.info(f"📋 Loaded {len(ignore_set)} ignore pattern(s) from .ignore file.")
    return ignore_set


def scanner_worker(source_directories, out_queue, ignore_set: set = None):
    """
    Producer thread. Walks through configured directories and queues up files.
    Skips files listed in the ignore_set (case-insensitive filename match).
    """
    if ignore_set is None:
        ignore_set = set()

    logger.info("📂 Scanner Thread: Started.")

    for folder in source_directories:
        if not os.path.exists(folder):
            logger.warning(f"⚠️ Configured directory does not exist, skipping: {folder}")
            continue

        logger.info(f"📂 Scanning: {folder}")
        for root, _, files in os.walk(folder):
            for file in files:
                if not file.lower().endswith(VALID_EXTENSIONS):
                    continue
                if file.lower().startswith('.trashed'):
                    continue
                if file.lower() in ignore_set:
                    logger.info(f"🚫 Ignoring file (in .ignore list): {file}")
                    continue

                filepath = os.path.join(root, file)
                filesize = os.path.getsize(filepath)

                out_queue.put({
                    "filename": file,
                    "filepath": filepath,
                    "filesize": filesize
                })

    # Signal completion
    out_queue.put(None)
    logger.info("📂 Scanner Thread: Finished scanning. Pushed termination signal.")
