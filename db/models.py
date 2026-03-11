# This file contains the schema definitions, table names and configurations.

# Table names
MEDIA_LIBRARY_TABLE = "media_library"
TRIP_CONFIG_TABLE = "trips_config"
DEVICE_CONFIG_TABLE = "device_config"

# Expected Schema of media_library (for reference or future ORM adoption)
MEDIA_LIBRARY_COLUMNS = [
    "sl_no",           # SERIAL (PK)
    "file_hash",       # TEXT (Indexed)
    "filename",        # TEXT (Indexed)
    "file_size_bytes", # BIGINT
    "upload_date",     # TEXT
    "account_email",   # TEXT
    "device_source",   # TEXT
    "remote_id",       # TEXT
    "album_name",      # TEXT
    "thumbid"          # TEXT
]

# Expected Schema of trips_config
TRIP_CONFIG_COLUMNS = [
    "name",            # TEXT (PK)
    "start",           # TEXT
    "end",             # TEXT
    "require_gps",     # BOOLEAN
    "album_id"         # TEXT
]

# Expected Schema of device_config
DEVICE_CONFIG_COLUMNS = [
    "device_name",     # TEXT (PK)
    "directories"      # TEXT (Comma-separated)
]
