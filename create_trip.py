import os
import requests
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# Setup Supabase Config
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    print("❌ Missing Supabase credentials in .env file!")
    exit(1)

BASE_URL = SUPABASE_URL.rstrip("/") + "/rest/v1"
HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=representation"
}


def check_trip_exists(name):
    """Check if a trip with the given name already exists in Supabase."""
    endpoint = f"{BASE_URL}/trips_config"
    params = {"name": f"eq.{name}", "select": "name"}
    try:
        resp = requests.get(endpoint, headers=HEADERS, params=params, timeout=10)
        if resp.status_code == 200 and len(resp.json()) > 0:
            return True
    except Exception as e:
        print(f"⚠️ Warning: Could not verify if trip exists due to connection error: {e}")
    return False


def validate_date(date_str):
    """Validate that the string matches YYYY-MM-DD."""
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        return True
    except ValueError:
        return False


def main():
    print("🌟 Create a New Trip Configuration 🌟")
    print("-" * 40)
    
    # 1. Trip Name
    while True:
        name = input("Enter Trip Name: ").strip()
        if not name:
            print("❌ Name cannot be empty.")
            continue
            
        print("Checking for conflicts...")
        if check_trip_exists(name):
            print(f"❌ A trip with the name '{name}' already exists in the database.")
            print("Please use a different name.")
        else:
            break
            
    # 2. Start Date
    while True:
        start = input("Enter Start Date (YYYY-MM-DD): ").strip()
        if validate_date(start):
            break
        print("❌ Invalid date format. Please use YYYY-MM-DD.")
        
    # 3. End Date
    while True:
        end = input("Enter End Date (YYYY-MM-DD): ").strip()
        if validate_date(end):
            if end >= start:
                break
            else:
                print("❌ End Date cannot be before Start Date.")
        else:
            print("❌ Invalid date format. Please use YYYY-MM-DD.")
            
    # 4. Require GPS
    while True:
        gps_input = input("Require GPS metadata for photos? (y/N): ").strip().lower()
        if gps_input in ['y', 'yes', 'true', '1']:
            require_gps = True
            break
        elif gps_input in ['n', 'no', 'false', '0', '']:
            require_gps = False
            break
        else:
            print("❌ Please enter 'y' or 'n'.")
            
    # 5. Album ID (Optional)
    album_id = input("Enter Album ID (Press Enter to skip & auto-create later): ").strip()
    if not album_id:
        album_id = None
        
    # Confirmation Prompt
    print("\n--- Summary ---")
    print(f"Name:        {name}")
    print(f"Start Date:  {start}")
    print(f"End Date:    {end}")
    print(f"Require GPS: {require_gps}")
    print(f"Album ID:    {album_id if album_id else 'None (Will auto-create in Google Photos)'}")
    
    confirm = input("\nDo you want to save this trip to the database? (Y/n): ").strip().lower()
    if confirm in ['n', 'no']:
        print("❌ Aborted.")
        return
        
    # Insert sequence
    print("⏳ Saving to Supabase...")
    payload = {
        "name": name,
        "start": start,
        "end": end,
        "require_gps": require_gps,
        "album_id": album_id
    }
    
    endpoint = f"{BASE_URL}/trips_config"
    try:
        resp = requests.post(endpoint, headers=HEADERS, json=payload, timeout=10)
        if resp.status_code in [200, 201, 204]:
            print(f"✅ Trip '{name}' successfully securely saved to the cloud!")
            print("The changes will be automatically fetched the next time `main_sql.py` runs.")
        else:
            print(f"❌ Failed to create trip: HTTP {resp.status_code} - {resp.text}")
    except Exception as e:
        print(f"❌ Failed to create trip due to connection error: {e}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n❌ Process interrupted by user. Exiting.")
