import os
import re
import argparse

def cleanup_files(directory, dry_run=True):
    """
    Scans a directory for files that end with '+<integer>' before the file extension
    and deletes them.
    """
    # Pattern to match: filename + <integer> . extension
    # Example: myphoto+1.jpg, myphoto+12.png
    # Using \+ to escape the plus sign, \d+ for one or more digits, 
    # and \.[^.]+ for the extension.
    pattern = re.compile(r".+\+\d+\.[^.]+$")
    
    count = 0
    if not os.path.isdir(directory):
        print(f"Error: {directory} is not a valid directory.")
        return

    print(f"Scanning directory: {directory}")
    print(f"Mode: {'DRY RUN (No files will be deleted)' if dry_run else 'EXECUTION (Files will be DELETED)'}")
    print("-" * 50)

    try:
        files = os.listdir(directory)
    except Exception as e:
        print(f"Error listing directory: {e}")
        return

    for filename in files:
        filepath = os.path.join(directory, filename)
        
        # Ensure it's a file and matches the pattern
        if os.path.isfile(filepath) and pattern.match(filename):
            if dry_run:
                print(f"[DRY RUN] Would delete: {filename}")
            else:
                try:
                    os.remove(filepath)
                    print(f"Deleted: {filename}")
                except Exception as e:
                    print(f"Error deleting {filename}: {e}")
            count += 1
            
    print("-" * 50)
    print(f"Total files {'found' if dry_run else 'deleted'}: {count}")
    if dry_run and count > 0:
        print("\nTo actually delete these files, run with the --run flag.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Remove files ending with '+<integer>' before the extension.")
    parser.add_argument("directory", help="The directory to scan for files.")
    parser.add_argument("--run", action="store_true", help="Actually delete the files (default is dry run).")
    
    args = parser.parse_args()
    
    cleanup_files(args.directory, dry_run=not args.run)
