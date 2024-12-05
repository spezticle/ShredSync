#!/spindles/shred/.venv/shredsync/bin/python
# Shred Match 1.18 with SQLite Integration

import os
import re
import sqlite3
import argparse
import yaml
from datetime import datetime, timedelta
from pathlib import Path

DB_FILE = "shredsync.db"

CREATE_SHREDMATCH_TABLE = """
CREATE TABLE IF NOT EXISTS shredmatch (
    unique_id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp_inserted TIMESTAMP,
    shredded_timestamp TIMESTAMP,
    media_source_folder TEXT,
    shredlog_source_folder TEXT,
    shred_UUID TEXT,
    import_UUID TEXT,
    import_folder_name TEXT,
    matching_log_filename TEXT,
    file_basename_list TEXT,
    wholename TEXT,
    full_name TEXT,
    first_name TEXT,
    last_name TEXT,
    email TEXT
);
"""

CREATE_METADATA_TABLE = """
CREATE TABLE IF NOT EXISTS metadata (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    shredmatch_id INTEGER,
    file_name TEXT,
    metadata_json TEXT,
    FOREIGN KEY (shredmatch_id) REFERENCES shredmatch(unique_id)
);
"""

CREATE_CHECKSUM_TABLE = """
CREATE TABLE IF NOT EXISTS checksum (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    shredmatch_id INTEGER,
    file_name TEXT,
    checksum TEXT,
    FOREIGN KEY (shredmatch_id) REFERENCES shredmatch(unique_id)
);
"""

def initialize_db():
    """Initialize the SQLite database and ensure tables exist."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute(CREATE_SHREDMATCH_TABLE)
    cursor.execute(CREATE_METADATA_TABLE)
    cursor.execute(CREATE_CHECKSUM_TABLE)
    conn.commit()
    conn.close()

def insert_into_shredmatch(data):
    """Insert data into the shredmatch table."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO shredmatch (
            timestamp_inserted, shredded_timestamp, media_source_folder, shredlog_source_folder,
            shred_UUID, import_UUID, import_folder_name, matching_log_filename, file_basename_list,
            wholename, full_name, first_name, last_name, email
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        data.get("shredded_timestamp"),
        data.get("media_source_folder"),
        data.get("shredlog_source_folder"),
        data.get("shred_UUID"),
        data.get("import_UUID"),
        data.get("import_folder_name"),
        data.get("matching_log_filename"),
        "\n".join(data.get("file_basename_list", [])),
        data.get("wholename"),
        data.get("full_name"),
        data.get("first_name"),
        data.get("last_name"),
        data.get("email"),
    ))
    conn.commit()
    conn.close()

def parse_arguments():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Match files with UUID and corresponding log entries.")
    parser.add_argument("--source", required=True, help="Path to the source folder.")
    parser.add_argument("--source-recursive", choices=["yes", "no"], default="no",
                        help="Whether to search source folder recursively.")
    parser.add_argument("--shredlogs", required=True, help="Path to the shred logs folder (non-recursive).")
    parser.add_argument("--imports", required=True, help="Path to the imports folder.")
    parser.add_argument("--imports-recursive", choices=["yes", "no"], default="no",
                        help="Whether to search imports folder recursively.")
    parser.add_argument("--destination", required=True, help="Path to the processing output.")
    parser.add_argument("--config", default="config.yaml", help="Path to the configuration file (defaults to config.yaml).")
    return parser.parse_args()

def get_files_from_source(source, recursive):
    """Get relevant files (.mp4, .zip, .jpg) from the source folder."""
    file_patterns = [r".*\.mp4$", r".*\.zip$", r".*\.jpg$"]
    files = []
    if recursive == "yes":
        for root, _, filenames in os.walk(source):
            for file in filenames:
                if any(re.search(pattern, file, re.IGNORECASE) for pattern in file_patterns):
                    files.append(os.path.join(root, file))
    else:
        for file in os.listdir(source):
            if any(re.search(pattern, file, re.IGNORECASE) for pattern in file_patterns):
                files.append(os.path.join(source, file))
    return files

def find_import_folders(import_path, recursive):
    """Find all import folders."""
    if recursive == "yes":
        return [os.path.join(root, dir_) for root, dirs, _ in os.walk(import_path) for dir_ in dirs if dir_.startswith("import-")]
    else:
        return [os.path.join(import_path, dir_) for dir_ in os.listdir(import_path) if dir_.startswith("import-") and os.path.isdir(os.path.join(import_path, dir_))]

def extract_uuid_and_date(filename):
    """Extract UUID and timestamp from a photo file name."""
    pattern = r"photo-(\d{12})-([A-Fa-f0-9-]{36})\.zip"
    match = re.search(pattern, filename, re.IGNORECASE)
    if match:
        raw_date, uuid = match.groups()
        try:
            timestamp = datetime.strptime(raw_date, "%Y%m%d%H%M")
            return uuid, timestamp
        except ValueError:
            return None, None
    return None, None

def main():
    args = parse_arguments()
    initialize_db()

    source = args.source
    shredlogs = args.shredlogs
    imports = args.imports
    imports_recursive = args.imports_recursive
    destination = args.destination

    source_files = get_files_from_source(source, args.source_recursive)
    import_folders = find_import_folders(imports, imports_recursive)

    for file in source_files:
        if "photo-" in file.lower():
            uuid, timestamp = extract_uuid_and_date(os.path.basename(file))
            if uuid and timestamp:
                print(f"Processing UUID: {uuid}, Timestamp: {timestamp}")
                # Placeholder for database checks and data processing
                # Insert into DB with partial data for now
                insert_into_shredmatch({
                    "shredded_timestamp": timestamp,
                    "media_source_folder": source,
                    "shredlog_source_folder": shredlogs,
                    "shred_UUID": uuid,
                    "file_basename_list": [os.path.basename(f) for f in source_files]
                })

if __name__ == "__main__":
    main()
