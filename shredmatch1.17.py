#!/spindles/shred/.venv/shredsync/bin/python
# Shred Match with SQLite Integration

import os
import re
import sqlite3
import argparse
import yaml
import hashlib
import json
from datetime import datetime, timedelta
from pathlib import Path


DB_FILE = "shredsync.db"

# SQL to create tables
CREATE_SHREDMATCH_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS shredmatch (
    unique_id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp_inserted TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    shredded_timestamp TIMESTAMP,
    media_source_folder TEXT,
    shredlog_source_folder TEXT,
    shred_UUID TEXT,
    import_UUID TEXT UNIQUE,
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

CREATE_METADATA_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS metadata (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    file_basename TEXT,
    metadata JSON,
    UNIQUE(file_basename)
);
"""

CREATE_CHECKSUM_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS checksum (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    file_basename TEXT,
    checksum TEXT,
    UNIQUE(file_basename)
);
"""


def initialize_db():
    """Initialize the SQLite database and ensure the tables exist."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute(CREATE_SHREDMATCH_TABLE_SQL)
    cursor.execute(CREATE_METADATA_TABLE_SQL)
    cursor.execute(CREATE_CHECKSUM_TABLE_SQL)
    conn.commit()
    conn.close()


def insert_data_to_db(data):
    """Insert processed data into the shredmatch table."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("""
        INSERT OR IGNORE INTO shredmatch (
            shredded_timestamp, media_source_folder, shredlog_source_folder,
            shred_UUID, import_UUID, import_folder_name, matching_log_filename,
            file_basename_list, wholename, full_name, first_name, last_name, email
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
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


def insert_file_metadata(file_basename, metadata):
    """Insert metadata for a file into the metadata table."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("""
        INSERT OR IGNORE INTO metadata (file_basename, metadata)
        VALUES (?, ?)
    """, (file_basename, json.dumps(metadata)))
    conn.commit()
    conn.close()


def insert_file_checksum(file_basename, checksum):
    """Insert checksum for a file into the checksum table."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("""
        INSERT OR IGNORE INTO checksum (file_basename, checksum)
        VALUES (?, ?)
    """, (file_basename, checksum))
    conn.commit()
    conn.close()


def load_config(config_file):
    """Load configuration from a YAML file."""
    try:
        with open(config_file, "r") as file:
            return yaml.safe_load(file)
    except FileNotFoundError:
        return {}
    except yaml.YAMLError as e:
        sys.exit(f"Error reading configuration file: {e}")


def parse_arguments():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Match files with UUID and corresponding log entries.")
    parser.add_argument("--source", required=True, help="Path to the source folder.")
    parser.add_argument("--source-recursive", choices=["yes", "no"], default="no",
                        help="Whether to search source folder recursively.")
    parser.add_argument("--shredlogs", required=True, help="Path to the shred logs folder (non-recursive).")
    parser.add_argument("--destination", required=True, help="Path to the processing output.")
    parser.add_argument("--config", default="config.yaml", help="Path to the configuration file (defaults to config.yaml).")
    return parser.parse_args()


def compute_checksum(file_path):
    """Compute SHA-256 checksum for a given file."""
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        while chunk := f.read(8192):
            sha256.update(chunk)
    return sha256.hexdigest()


def extract_metadata(file_path):
    """Extract metadata from an `.mp4` or `.jpg` file."""
    # Placeholder: Implement metadata extraction logic using tools like `ffprobe` or `Pillow`
    return {"placeholder_key": "placeholder_value"}


def get_files_from_source(source, recursive):
    """Get relevant files (.mp4, .zip, .jpg) from the source folder."""
    file_patterns = [r".*\.mp4$", r".*\.zip$", r".*\.jpg$"]
    files = []
    if recursive:
        for root, _, filenames in os.walk(source):
            for file in filenames:
                if any(re.search(pattern, file, re.IGNORECASE) for pattern in file_patterns):
                    files.append(os.path.join(root, file))
    else:
        for file in os.listdir(source):
            if any(re.search(pattern, file, re.IGNORECASE) for pattern in file_patterns):
                files.append(os.path.join(source, file))
    return files


def process_files(source_files, shredlogs_path):
    """Process files and store results in the database."""
    for file_path in source_files:
        # Extract UUID, timestamp, etc.
        # Placeholder: Continue building this logic
        checksum = compute_checksum(file_path)
        insert_file_checksum(os.path.basename(file_path), checksum)

        if file_path.endswith((".mp4", ".jpg")):
            metadata = extract_metadata(file_path)
            insert_file_metadata(os.path.basename(file_path), metadata)


def main():
    args = parse_arguments()
    config = load_config(args.config)

    # Merge arguments with config file (arguments take precedence)
    source = args.source or config.get("source")
    source_recursive = args.source_recursive.lower() == "yes"
    shredlogs = args.shredlogs or config.get("shredlogs")
    destination = args.destination or config.get("destination")

    if not (source and shredlogs and destination):
        sys.exit("Error: Missing required parameters (source, shredlogs, destination).")

    # Initialize database
    initialize_db()

    # Collect files from source
    source_files = get_files_from_source(source, source_recursive)

    # Process files
    process_files(source_files, shredlogs)


if __name__ == "__main__":
    main()
