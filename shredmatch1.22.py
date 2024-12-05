#!/spindles/shred/.venv/shredsync/bin/python
# Shred Match 1.22

import os
import re
import sqlite3
import argparse
import yaml
from datetime import datetime, timedelta
from pathlib import Path

DB_FILE = "shredsync.db"


def create_database():
    """Create the SQLite database and tables."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS shredmatch (
        unique_id INTEGER PRIMARY KEY AUTOINCREMENT,
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

    conn.commit()
    conn.close()


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


def load_config(config_file):
    """Load configuration from a YAML file."""
    try:
        with open(config_file, "r") as file:
            return yaml.safe_load(file)
    except FileNotFoundError:
        return {}
    except yaml.YAMLError as e:
        sys.exit(f"Error reading configuration file: {e}")


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
    """Retrieve relevant files (.mp4, .zip, .jpg) from the source folder."""
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


def search_uuid_in_logs(uuid, log_files, logs_path):
    """Search for UUID in the specified log files."""
    import_folder = None
    wholename = None
    matching_log = None
    pattern = rf"starting reading {uuid} .*(/import-[\w-]+)"

    for log_file in log_files:
        log_path = os.path.join(logs_path, log_file)
        if not os.path.isfile(log_path):  # Ensure it's a file
            continue

        with open(log_path, "r") as log:
            for line in log:
                match = re.search(pattern, line, re.IGNORECASE)
                if match:
                    import_folder = os.path.basename(match.group(1))
                    wholename_match = re.search(r"import-([A-Za-z]+[A-Za-z0-9]*)-", import_folder)
                    wholename = wholename_match.group(1) if wholename_match else None
                    matching_log = log_file
                    return import_folder, wholename, matching_log
    return None, None, None


def main():
    create_database()

    args = parse_arguments()
    config = load_config(args.config)
    settings = merge_config_with_args(args, config)

    shredded_files = get_files_from_source(settings["shredded_source"], settings["shredded_source_recursive"])
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    for file in shredded_files:
        if "photo-" in file.lower():
            uuid, timestamp = extract_uuid_and_date(file)
            if uuid and timestamp:
                related_files = [file for file in shredded_files if uuid in file]
                log_files = [
                    log for log in os.listdir(settings["shred_log_source"])
                    if re.search(r"com\.shredvideo\.ShredCentral", log, re.IGNORECASE)
                ]
                import_folder, wholename, matching_log = search_uuid_in_logs(uuid, log_files, settings["shred_log_source"])
                if matching_log:
                    entry = (
                        datetime.now().isoformat(),
                        timestamp.isoformat(),
                        settings["shredded_source"],
                        settings["import_source"],
                        settings["shred_log_source"],
                        uuid,
                        None,
                        import_folder,
                        None,
                        matching_log,
                        ", ".join(related_files),
                        wholename,
                        None, None, None, None
                    )
                    cursor.execute("""
                    INSERT INTO shredmatch (
                        timestamp_inserted, shredded_timestamp, shredded_source_folder, import_source_folder,
                        shredlog_source_folder, shredded_UUID, import_UUID, import_folder_name, import_folder_files,
                        matching_log_filename, shredded_files, wholename, full_name, first_name, last_name, email
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, entry)
    conn.commit()
    conn.close()
    print("Database has been updated.")


if __name__ == "__main__":
    main()
