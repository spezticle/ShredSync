#!/spindles/shred/.venv/shredsync/bin/python
# Shred Match with Database Integration

import os
import re
import sqlite3
import argparse
import yaml
from datetime import datetime, timedelta
from pathlib import Path

DB_FILE = "shredsync.db"

def create_database():
    """Create the SQLite database and necessary tables if they do not exist."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    # Create shredmatch table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS shredmatch (
        unique_id TEXT PRIMARY KEY,
        timestamp_inserted TIMESTAMP NOT NULL,
        shredded_timestamp TIMESTAMP,
        shredded_source_folder TEXT,
        import_source_folder TEXT,
        shredlog_source_folder TEXT,
        shredded_UUID TEXT,
        import_UUID TEXT,
        import_folder_name TEXT,
        import_folder_files TEXT,
        matching_log_filename TEXT,
        shredded_files TEXT,
        wholename TEXT,
        full_name TEXT,
        first_name TEXT,
        last_name TEXT,
        email TEXT
    );
    """)

    # Create metadata table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS metadata (
        file_id TEXT PRIMARY KEY,
        file_name TEXT NOT NULL,
        metadata_json TEXT,
        FOREIGN KEY(file_id) REFERENCES shredmatch(unique_id)
    );
    """)

    # Create checksum table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS checksum (
        file_id TEXT PRIMARY KEY,
        file_name TEXT NOT NULL,
        checksum_sha256 TEXT,
        FOREIGN KEY(file_id) REFERENCES shredmatch(unique_id)
    );
    """)

    conn.commit()
    conn.close()

def insert_shredmatch_entry(conn, entry):
    """Insert an entry into the shredmatch table."""
    cursor = conn.cursor()
    cursor.execute("""
    INSERT INTO shredmatch (
        unique_id, timestamp_inserted, shredded_timestamp, shredded_source_folder,
        import_source_folder, shredlog_source_folder, shredded_UUID, import_UUID,
        import_folder_name, import_folder_files, matching_log_filename, shredded_files,
        wholename, full_name, first_name, last_name, email
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, entry)
    conn.commit()

def generate_unique_id():
    """Generate a unique ID (zero-padded 8-digit identifier)."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM shredmatch")
    count = cursor.fetchone()[0] or 0
    conn.close()
    return f"{count + 1:08d}"

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

def get_files_from_source(source, recursive):
    """Get relevant files (.mp4, .zip, .jpg) from the source folder."""
    file_patterns = [r".*\.mp4$", r".*\.zip$", r".*\.jpg$"]
    files = []
    if recursive == "yes":
        for root, _, filenames in os.walk(source):
            for file in filenames:
                if any(re.search(pattern, file, re.IGNORECASE) for pattern in file_patterns):
                    files.append(file)  # Store only the filename
    else:
        for file in os.listdir(source):
            if any(re.search(pattern, file, re.IGNORECASE) for pattern in file_patterns):
                files.append(file)  # Store only the filename
    return files

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

def find_related_files(source_files, uuid):
    """Find all files related to the given UUID."""
    return [f for f in source_files if re.search(uuid, f, re.IGNORECASE)]

def find_corresponding_log(logs_path, timestamp):
    """Find the corresponding log file based on the timestamp."""
    log_pattern = r"com\.shredvideo\.ShredCentral (\d{4}-\d{2}-\d{2}) (\d{2}-\d{2})\.log"
    potential_logs = []
    for log_file in os.listdir(logs_path):
        match = re.search(log_pattern, log_file, re.IGNORECASE)
        if match:
            log_date, log_time = match.groups()
            log_datetime = datetime.strptime(f"{log_date} {log_time.replace('-', ':')}", "%Y-%m-%d %H:%M")
            potential_logs.append((log_datetime, log_file))

    result_logs = []
    for delta in [-2, -1, 0, 1]:
        target_date = timestamp + timedelta(days=delta)
        for log_datetime, log_file in potential_logs:
            if log_datetime.date() == target_date.date():
                result_logs.append(log_file)
                break
    return result_logs

def search_uuid_in_logs(uuid, log_files, logs_path):
    """Search for UUID in the specified log files."""
    import_folder = None
    name = None
    pattern = r"starting reading " + re.escape(uuid) + r" .*(/import-[\w-]+)"
    for log_file in log_files:
        log_path = os.path.join(logs_path, log_file)
        if not os.path.isfile(log_path):  # Ensure it's a file
            continue
        with open(log_path, "r") as log:
            for line in log:
                match = re.search(pattern, line, re.IGNORECASE)
                if match:
                    import_folder = os.path.basename(match.group(1))
                    name_match = re.search(r"import-([\w-]+?)-", import_folder)
                    name = name_match.group(1) if name_match else None
                    return import_folder, name
    return None, None

def main():
    create_database()
    args = parse_arguments()
    source = args.source
    shredlogs = args.shredlogs

    conn = sqlite3.connect(DB_FILE)

    source_files = get_files_from_source(source, args.source_recursive)
    for file in source_files:
        if "photo-" in file.lower():
            uuid, timestamp = extract_uuid_and_date(file)
            if uuid and timestamp:
                related_files = find_related_files(source_files, uuid)
                log_files = find_corresponding_log(shredlogs, timestamp)
                import_folder, name = search_uuid_in_logs(uuid, log_files, shredlogs)
                unique_id = generate_unique_id()
                timestamp_inserted = datetime.now().isoformat()

                entry = (
                    unique_id,
                    timestamp_inserted,
                    timestamp.isoformat() if timestamp else None,
                    source,
                    None,  # import source folder placeholder
                    shredlogs,
                    uuid,
                    None,  # import UUID placeholder
                    import_folder,
                    ", ".join(related_files),
                    ", ".join(log_files),
                    ", ".join(related_files),
                    name,
                    None,
                    None,
                    None,
                    None
                )
                insert_shredmatch_entry(conn, entry)

    conn.close()

if __name__ == "__main__":
    main()
