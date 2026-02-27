select distinct device_source from media_library;


update media_library set device_source = 'OnePlus_13R' where device_source = 'OnePlus 13R';

select count(*) from media_library where device_source = 'OPPO';

update trips_config set name='new_name' where name='old_name'






-- Create the main media_library table
CREATE TABLE media_library (
    sl_no SERIAL PRIMARY KEY,
    file_hash TEXT,
    filename TEXT,
    file_size_bytes BIGINT,
    upload_date TEXT,
    account_email TEXT,
    device_source TEXT,
    remote_id TEXT,
    album_name TEXT,
    thumbid TEXT
);

-- Create indexes to speed up the exists checks
CREATE INDEX idx_filename ON media_library(filename);
CREATE INDEX idx_hash ON media_library(file_hash);

-- Create the trips_config table
CREATE TABLE trips_config (
    name TEXT PRIMARY KEY,
    start TEXT,
    "end" TEXT,
    require_gps BOOLEAN,
    album_id TEXT
);

-- Create the device_config table
CREATE TABLE device_config (
    device_name TEXT PRIMARY KEY,
    directories TEXT,
    sl_no SERIAL
);
