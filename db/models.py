# This file contains the schema definitions, table names and configurations.

# Table names
MEDIA_LIBRARY_TABLE = "media_library"
TRIP_CONFIG_TABLE = "trips_config"
DEVICE_CONFIG_TABLE = "device_config"
STORAGE_SUMMARY_TABLE = "storage_summary"
ACCOUNT_DISTRIBUTION_TABLE = "account_distribution"
DEVICE_DISTRIBUTION_TABLE = "device_distribution"

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
    "album_name"       # TEXT
]

# Expected Schema of trips_config
TRIP_CONFIG_COLUMNS = [
    "name",            # TEXT (PK)
    "start",           # TEXT
    "end",             # TEXT
    "require_gps",     # BOOLEAN
    "album_id",        # TEXT
    "album_url",       # TEXT
    "email_message_id" # TEXT  — Message-ID of the album creation notification email
]

# Expected Schema of device_config
DEVICE_CONFIG_COLUMNS = [
    "device_name",     # TEXT (PK)
    "directories"      # TEXT (Comma-separated)
]

# Expected Schema of storage_summary
STORAGE_SUMMARY_COLUMNS = [
    "id",              # INTEGER (PK)
    "synced_at",       # TIMESTAMP
    "total_photos",    # INTEGER
    "total_videos",    # INTEGER
    "total_assets",    # INTEGER
    "total_photos_size_gb", # REAL
    "total_videos_size_gb", # REAL
    "total_size_gb"    # REAL
]

# Expected Schema of account_distribution
ACCOUNT_DISTRIBUTION_COLUMNS = [
    "id",              # INTEGER (PK)
    "summary_id",      # INTEGER (FK)
    "account_email",   # TEXT
    "photos_count",    # INTEGER
    "videos_count",    # INTEGER
    "photos_size_mb",  # REAL
    "videos_size_mb",  # REAL
    "total_size_mb",   # REAL
    "percentage"       # REAL
]

# Expected Schema of device_distribution
DEVICE_DISTRIBUTION_COLUMNS = [
    "id",              # INTEGER (PK)
    "summary_id",      # INTEGER (FK)
    "device_name",     # TEXT
    "photos_count",    # INTEGER
    "videos_count",    # INTEGER
    "total_size_mb",   # REAL
    "percentage"       # REAL
]
