import os
import sys
import socket
from infra.config_loader import get_config, set_config
from db.balancer import DatabaseBalancer

# Determine the absolute path to the config file in the project root
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def prompt_with_default(prompt: str, default_val: str = "") -> str:
    """Helper to prompt user with a default fallback."""
    if default_val is not None:
        v = input(f"{prompt} [{default_val}]: ").strip()
        return v if v else default_val
    else:
        v = input(f"{prompt}: ").strip()
        return v

def run_init_wizard():
    print("="*60)
    print("PhotoUploaderDB - Device Initialization Wizard")
    print("="*60)
    print("\nThis wizard will guide you through setting up this device.")
    
    current_device_name = get_config("app.device_name", socket.gethostname())
    current_nhost_url = get_config("database.nhost_url", "")
    current_neon_url = get_config("database.neon_url", "")
    
    print("\n--- 1. Environment Configuration ---")
    device_name = prompt_with_default("Enter a unique Device Name", current_device_name)
    
    print("\n[Database Connections]")
    nhost_url = prompt_with_default("Enter NHOST_DB_URL", current_nhost_url)
    neon_url = prompt_with_default("Enter NEON_DB_URL (Optional)", current_neon_url)
    
    print("\nSaving configuration to config.yaml...")
    set_config("app.device_name", device_name, save=False)
    set_config("database.nhost_url", nhost_url, save=False)
    set_config("database.nhost_enabled", True, save=False)
    if neon_url:
        set_config("database.neon_url", neon_url, save=False)
        set_config("database.neon_enabled", True, save=False)
    
    if get_config("app.service_name") is None:
        set_config("app.service_name", "Photo_Uploader", save=False)
    if get_config("app.dry_run") is None:
        set_config("app.dry_run", False, save=False)
    
    from infra.config_loader import Config
    Config.save()
    
    print("Note: Email notifications are also managed in config.yaml.")

    print("\n--- 2. Database Connection Check ---")
    try:
        db = DatabaseBalancer(use_local_cache=True)
        if db.check_connection():
            print("Successfully connected to configured databases.")
        else:
            print("Error: Could not establish database connection. Please check your URLs.")
            sys.exit(1)
    except Exception as e:
         print(f"Database initialization error: {e}")
         sys.exit(1)
         
    print("\n--- 3. Source Directories Configuration ---")
    print(f"Let's configure the folders to scan for photos on '{device_name}'.")
    print("We will ask for them one by one. Leave blank and press Enter when finished.")
    
    folders = []
    while True:
        folder = input("Enter a valid folder path (or press Enter to finish): ").strip()
        if not folder:
            break
        
        # Expand user path (~) if provided, and make absolute
        folder = os.path.abspath(os.path.expanduser(folder))
        
        if os.path.exists(folder) and os.path.isdir(folder):
            if folder not in folders:
                folders.append(folder)
                print(f"  Added: {folder}")
            else:
                 print("  Warning: Folder already added in this session.")
        else:
            print(f"  Error: Invalid path or not a directory: {folder}")
            
    if not folders:
        print("Warning: No folders added. You must configure folders via `python main.py init` or `db/query_db.py` later to upload photos.")
    else:
        dirs_string = ",".join(folders)
        
        print("\nSaving configured directories to cloud database...")
        try:
             # UPSERT LOGIC
             sql = """
             INSERT INTO device_config (device_name, directories, updated_at) 
             VALUES (%s, %s, CURRENT_TIMESTAMP) 
             ON CONFLICT (device_name) 
             DO UPDATE SET 
                directories = EXCLUDED.directories,
                updated_at = CURRENT_TIMESTAMP
             """
             db.execute_query(sql, (device_name, dirs_string), is_write=True)
             
             # Also update local cache for immediate availability (thread-safe)
             db.upsert_device_config_local(device_name, dirs_string)
                 
             print(f"Successfully registered {len(folders)} folders for '{device_name}'.")
        except Exception as e:
             print(f"Failed to save to database: {e}")
             
    print("\n" + "="*60)
    print("Initialization Complete!")
    print(f"Your device '{device_name}' is now configured.")
    print("You can start the uploader by running: python main.py")
    print("="*60 + "\n")

if __name__ == "__main__":
    run_init_wizard()
