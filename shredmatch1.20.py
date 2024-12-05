#!/spindles/shred/.venv/shredsync/bin/python
# Shred Match 1.20 with Enhanced Database and Argument Handling

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
    parser.add_argument("--shredded_source", required=True, help="Path to the shredded source folder.")
    parser.add_argument("--shredded_source-recursive", choices=["yes", "no"], default="no",
                        help="Whether to search the shredded source folder recursively.")
    parser.add_argument("--import_source", required=True, help="Path to the import source folder.")
    parser.add_argument("--import_source-recursive", choices=["yes", "no"], default="no",
                        help="Whether to search the import source folder recursively.")
    parser.add_argument("--shred_log_source", required=True, help="Path to the shred logs folder.")
    parser.add_argument("--destination", required=True, help="Path to the processing output.")
    parser.add_argument("--config", default="config.yaml", help="Path to the configuration file (defaults to config.yaml).")
    return parser.parse_args()

def merge_config_with_args(args, config):
    """Merge arguments with configuration file values, prioritizing arguments."""
    merged = {
        "shredded_source": args.shredded_source or config.get("shredded_source"),
        "shredded_source_recursive": args.shredded_source_recursive or config.get("shredded_source-recursive", "no"),
        "import_source": args.import_source or config.get("import_source"),
        "import_source_recursive": args.import_source_recursive or config.get("import_source-recursive", "no"),
        "shred_log_source": args.shred_log_source or config.get("shred_log_source"),
        "destination": args.destination or config.get("destination"),
    }

    for key, value in merged.items():
        if not value:
            sys.exit(f"Error: Missing required parameter: {key}")
    return merged

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

def main():
    create_database()
    args = parse_arguments()
    config = load_config(args.config)
    settings = merge_config_with_args(args, config)

    shredded_source = settings["shredded_source"]
    shredded_recursive = settings["shredded_source_recursive"]
    import_source = settings["import_source"]
    import_recursive = settings["import_source_recursive"]
    shred_log_source = settings["shred_log_source"]

    shredded_files = get_files_from_source(shredded_source, shredded_recursive)
    conn = sqlite3.connect(DB_FILE)

    for file in shredded_files:
        if "photo-" in file.lower():
            uuid, timestamp = extract_uuid_and_date(file)
            if uuid and timestamp:
                related_files = find_related_files(shredded_files, uuid)
                log_files = find_corresponding_log(shred_log_source, timestamp)
                unique_id = f"{len(related_files):08d}"
                timestamp_inserted = datetime.now().isoformat()

                entry = (
                    unique_id,
                    timestamp_inserted,
                    timestamp.isoformat(),
                    shredded_source,
                    import_source,
                    shred_log_source,
                    uuid,
                    None,
                    None,
                    ", ".join(related_files),
                    ", ".join(log_files),
                    ", ".join(related_files),
                    None,
                    None,
                    None,
                    None,
                    None
                )

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

    conn.close()

if __name__ == "__main__":
    main()
