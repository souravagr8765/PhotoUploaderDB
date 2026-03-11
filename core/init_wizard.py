import os
import sys
import socket
from dotenv import load_dotenv, set_key
from db.balancer import DatabaseBalancer

# Determine the absolute path to the .env file in the project root
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ENV_FILE_PATH = os.path.join(BASE_DIR, ".env")

def prompt_with_default(prompt: str, default_val: str = "") -> str:
    """Helper to prompt user with a default fallback."""
    if default_val:
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
    
    # 1. Load current .env values (if any)
    load_dotenv(ENV_FILE_PATH)
    
    current_device_name = os.getenv("DEVICE_NAME", socket.gethostname())
    current_nhost_url = os.getenv("NHOST_DB_URL", "")
    current_neon_url = os.getenv("NEON_DB_URL", "")
    current_sender_email = os.getenv("SENDER_EMAIL", "")
    current_app_password = os.getenv("APP_PASSWORD", "")
    current_receiver_email = os.getenv("RECEIVER_EMAIL", current_sender_email)
    
    print("\n--- 1. Environment Configuration ---")
    device_name = prompt_with_default("Enter a unique Device Name", current_device_name)
    
    print("\n[Database Connections]")
    nhost_url = prompt_with_default("Enter NHOST_DB_URL", current_nhost_url)
    neon_url = prompt_with_default("Enter NEON_DB_URL (Optional)", current_neon_url)
    
    print("\n[Email Notifications (Optional but recommended)]")
    sender_email = prompt_with_default("Enter Sender Gmail Address", current_sender_email)
    
    app_password = current_app_password
    if sender_email:
        app_pass_display = "*****" if current_app_password else ""
        new_app_password = prompt_with_default("Enter 16-character Gmail App Password", app_pass_display)
        if new_app_password and new_app_password != "*****":
            app_password = new_app_password
            
        receiver_email = prompt_with_default("Enter Receiver Email (Usually same as sender)", current_receiver_email or sender_email)
    else:
        receiver_email = ""
    
    # Create or update .env file
    if not os.path.exists(ENV_FILE_PATH):
        with open(ENV_FILE_PATH, "w") as f:
            f.write("# PhotoUploaderDB Auto-Generated .env\n")
            
    print("\nSaving configuration to .env...")
    set_key(ENV_FILE_PATH, "DEVICE_NAME", device_name)
    set_key(ENV_FILE_PATH, "NHOST_DB_URL", nhost_url)
    if neon_url:
        set_key(ENV_FILE_PATH, "NEON_DB_URL", neon_url)
    
    if sender_email:
        set_key(ENV_FILE_PATH, "SENDER_EMAIL", sender_email)
        set_key(ENV_FILE_PATH, "APP_PASSWORD", app_password)
        set_key(ENV_FILE_PATH, "RECEIVER_EMAIL", receiver_email)
        
    set_key(ENV_FILE_PATH, "SMTP_SERVER", "smtp.gmail.com")
    set_key(ENV_FILE_PATH, "SMTP_PORT", "587")

    if not os.getenv("SERVICE_NAME"):
        set_key(ENV_FILE_PATH, "SERVICE_NAME", "Photo_Uploader")
    if not os.getenv("DRY_RUN"):
        set_key(ENV_FILE_PATH, "DRY_RUN", "False")
        
    # Reload environment so DatabaseBalancer picks up new changes.
    load_dotenv(ENV_FILE_PATH, override=True)
    
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
             INSERT INTO device_config (device_name, directories) 
             VALUES (%s, %s) 
             ON CONFLICT (device_name) 
             DO UPDATE SET directories = EXCLUDED.directories
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
