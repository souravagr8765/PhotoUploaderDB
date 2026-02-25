import os
import sys
import json
import logging
import logging
import pg8000.native
from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database import DatabaseManager

load_dotenv()

# Setup Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("QueryTool")

def check_trip_exists(db, name):
    """Check if a trip with the given name already exists in the Database."""
    sql = "SELECT name FROM trips_config WHERE name = %s LIMIT 1"
    try:
        res = db.execute_query(sql, (name,), fetch_one=True)
        if res:
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

def create_trip(db):
    print("\n🌟 Create a New Trip Configuration 🌟")
    print("-" * 40)
    
    # 1. Trip Name
    while True:
        name = input("Enter Trip Name: ").strip()
        if not name:
            print("❌ Name cannot be empty.")
            continue
            
        print("Checking for conflicts...")
        if check_trip_exists(db, name):
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
    print("⏳ Saving to Cloud...")
    try:
        sql = "INSERT INTO trips_config (name, start, \"end\", require_gps, album_id) VALUES (%s, %s, %s, %s, %s)"
        params = (name, start, end, require_gps, album_id)
        db.execute_query(sql, params, is_write=True)
        print(f"✅ Trip '{name}' successfully securely saved to the cloud!")
        print("The changes will be automatically fetched the next time `main_sql.py` runs.")
    except Exception as e:
        print(f"❌ Failed to create trip due to connection error: {e}")

def execute_raw_sql(query):
    nhost_url = os.getenv("NHOST_DB_URL")
    if not nhost_url:
        print("❌ Missing NHOST_DB_URL in .env required for raw SQL queries.")
        return
        
    try:
        # Re-use parsing logic or parse natively here
        import urllib.parse
        parsed = urllib.parse.urlparse(nhost_url)
        con = pg8000.native.Connection(
            user=parsed.username,
            host=parsed.hostname,
            database=parsed.path.lstrip('/'),
            port=parsed.port or 5432,
            password=parsed.password
        )
        print("⏳ Executing query on Nhost...")
        res = con.run(query)
        columns = [col['name'] for col in con.columns] if con.columns else []
        
        if columns and res:
            # Format and print as JSON-like list of dicts
            result_list = [dict(zip(columns, row)) for row in res]
            import datetime as dt
            def default_serializer(obj):
                if isinstance(obj, (dt.date, dt.datetime)):
                    return obj.isoformat()
                return str(obj)
                
            print(f"\n✅ Query successful. Found {len(result_list)} records:\n")
            print(json.dumps(result_list, indent=2, default=default_serializer))
        else:
            print(f"\n✅ Query executed successfully. Rows affected: {con.row_count}")
            
        con.close()
    except Exception as e:
        print(f"❌ SQL Execution Error: {e}")

def run_query():
    print("🚀 Database Query Tool")
    print("----------------------")
    
    try:
        db = DatabaseManager(use_local_cache=False) # Always query cloud for this tool
        print("✅ Connected to Database")
    except Exception as e:
        print(f"❌ Connection Failed: {e}")
        return

    while True:
        print("\nOptions:")
        print("1. Count Total Records")
        print("2. Search by Filename")
        print("3. Search by Album Name")
        print("4. Create a New Trip")
        print("5. Custom Query (Raw SQL)")
        print("6. Exit")
        
        choice = input("\nEnter choice (1-6): ").strip()
        
        if choice == "1":
            try:
                sql = "SELECT COUNT(*) FROM media_library"
                res = db.execute_query(sql, fetch_one=True)
                if res:
                    print(f"\n📊 Total Records: {res[0]}")
                else:
                    print("\n⚠️ Could not determine count.")
            except Exception as e:
                print(f"Error: {e}")

        elif choice == "2":
            filename = input("Enter filename (or part of it): ").strip()
            sql = "SELECT sl_no, file_hash, filename, file_size_bytes, upload_date, account_email, device_source, album_name FROM media_library WHERE filename ILIKE %s"
            _exec_request(db, sql, (f"%{filename}%",))

        elif choice == "3":
            album = input("Enter album name: ").strip()
            sql = "SELECT sl_no, file_hash, filename, file_size_bytes, upload_date, account_email, device_source, album_name FROM media_library WHERE album_name = %s"
            _exec_request(db, sql, (album,))

        elif choice == "4":
            create_trip(db)
            
        elif choice == "5":
            print("\nEnter a raw SQL query (e.g., SELECT * FROM media_library WHERE file_size_bytes > 1000000)")
            query = input("Query: ").strip()
            if query:
                execute_raw_sql(query)

        elif choice == "6":
            print("👋 Bye!")
            break

def _exec_request(db, sql, params):
    try:
        rows = db.execute_query(sql, params, fetch_all=True)
        
        if rows is not None:
            # We don't have column names easily from execute_query when using DB API 2.0 unless we inspect the cursor, 
            # but for a quick tool we can just print the tuples or manually map known columns.
            # To make it nice, we'll just print rows nicely
            print(f"\n✅ Found {len(rows)} records:\n")
            for r in rows:
                 print(r)
        else:
            print(f"❌ Query returned nothing.")
    except Exception as e:
        print(f"❌ Request Failed: {e}")

if __name__ == "__main__":
    run_query()
