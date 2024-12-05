#!/spindles/shred/.venv/shredsync/bin/python
# shredmatch1.24.py
import os
import re
import sqlite3
import argparse
import yaml
import logging
from datetime import datetime, timedelta
from pathlib import Path

# Logging and database file configuration
CONFIG_FILE = "config.yaml"

def load_config(config_file):
    """Load configuration from a YAML file."""
    try:
        with open(config_file, "r") as file:
            return yaml.safe_load(file)
    except FileNotFoundError:
        print(f"Configuration file {config_file} not found.")
        exit(1)
    except yaml.YAMLError as e:
        print(f"Error reading configuration file: {e}")
        exit(1)

def setup_logging(log_file, log_format):
    """Setup logging."""
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format=log_format,
    )
    logging.getLogger().addHandler(logging.StreamHandler())

def parse_arguments():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="ShredMatch file and log matcher.")
    parser.add_argument("--shredded_source", help="Path to the shredded source folder.")
    parser.add_argument("--shredded_source-recursive", choices=["yes", "no"], help="Search shredded source recursively.")
    parser.add_argument("--import_source", help="Path to the import source folder.")
    parser.add_argument("--import_source-recursive", choices=["yes", "no"], help="Search import source recursively.")
    parser.add_argument("--shred_log_source", help="Path to the shred logs folder.")
    parser.add_argument("--destination", help="Path to the processing output.")
    parser.add_argument("--config", default=CONFIG_FILE, help="Path to configuration file.")
    return parser.parse_args()

def merge_config_with_args(args, config):
    """Merge command-line arguments with config file, prioritizing arguments."""
    merged = {
        "shredded_source": args.shredded_source or config.get("shredded_source"),
        "shredded_source_recursive": args.shredded_source_recursive or config.get("shredded_source-recursive"),
        "import_source": args.import_source or config.get("import_source"),
        "import_source_recursive": args.import_source_recursive or config.get("import_source-recursive"),
        "shred_log_source": args.shred_log_source or config.get("shred_log_source"),
        "destination": args.destination or config.get("destination"),
        "shredmatch_logfile": config.get("shredmatch_logfile"),
        "log_format": config.get("log_format"),
        "shredmatch_db_file": config.get("shredmatch_db_file"),
    }
    for key, value in merged.items():
        if not value:
            print(f"Error: Missing required parameter {key}.")
            exit(1)
    return merged

def create_database(db_file):
    """Create the database if it does not exist."""
    if not os.path.exists(db_file):
        logging.info(f"Database not found. Creating database at {db_file}.")
        conn = sqlite3.connect(db_file)
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
            wholename TEXT
        );
        """)
        conn.commit()
        conn.close()

def get_files_from_source(source, recursive):
    """Get relevant files (.mp4, .zip, .jpg) from a folder."""
    logging.info(f"Looking in shredded_source: {source}")
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
    logging.info(f"Found {len(files)} qualifying files.")
    return files

def extract_uuid_and_date(filename):
    """Extract UUID and timestamp from a photo file name."""
    pattern = r"photo-(\d{12})-([A-Fa-f0-9-]{36})\.zip"
    match = re.search(pattern, filename, re.IGNORECASE)
    if match:
        raw_date, uuid = match.groups()
        try:
            timestamp = datetime.strptime(raw_date, "%Y%m%d%H%M")
            logging.info(f"Extracted UUID: {uuid}, Timestamp: {timestamp} from file: {filename}")
            return uuid, timestamp
        except ValueError:
            logging.warning(f"Invalid date format in file: {filename}")
            return None, None
    logging.warning(f"No UUID and date found in file: {filename}")
    return None, None

def search_uuid_in_logs(uuid, logs_path):
    """Search for UUID in log files and return matching log file, import folder, and name."""
    log_pattern = re.compile(rf"starting reading {uuid} .*(/import-[\w-]+)")
    log_files = os.listdir(logs_path)
    logging.info(f"Found {len(log_files)} potential log files.")
    for idx, log_file in enumerate(log_files, start=1):
        logging.info(f"Searching log {idx}/{len(log_files)}: {log_file}")
        log_path = os.path.join(logs_path, log_file)
        if os.path.isfile(log_path):
            with open(log_path, "r") as log:
                for line in log:
                    match = log_pattern.search(line)
                    if match:
                        import_folder = os.path.basename(match.group(1))
                        wholename = re.search(r"import-([\w-]+?)-", import_folder)
                        name = wholename.group(1) if wholename else None
                        logging.info(f"Match found in log {log_file}: UUID={uuid}, Import Folder={import_folder}, Name={name}")
                        return log_file, import_folder, name
    logging.warning(f"No match found for UUID {uuid} in the logs.")
    return None, None, None

def process_files(config):
    """Process shredded files and log matches."""
    shredded_files = get_files_from_source(config["shredded_source"], config["shredded_source_recursive"])
    db_file = config["shredmatch_db_file"]

    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    for idx, file in enumerate(shredded_files, start=1):
        logging.info(f"Processing file {idx}/{len(shredded_files)}: {file}")
        uuid, timestamp = extract_uuid_and_date(file)
        if uuid and timestamp:
            log_file, import_folder, name = search_uuid_in_logs(uuid, config["shred_log_source"])
            if log_file:
                shredded_files = [f for f in shredded_files if uuid in f]
                timestamp_inserted = datetime.now()
                logging.info("Inserting values into database:")
                cursor.execute("""
                INSERT INTO shredmatch (
                    timestamp_inserted, shredded_timestamp, shredded_source_folder,
                    import_source_folder, shredlog_source_folder, shredded_UUID,
                    import_UUID, import_folder_name, import_folder_files,
                    matching_log_filename, shredded_files, wholename
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    timestamp_inserted, timestamp, config["shredded_source"],
                    config["import_source"], config["shred_log_source"], uuid,
                    None, import_folder, ", ".join(shredded_files), log_file,
                    ", ".join(shredded_files), name
                ))
                conn.commit()
                logging.info(f"Values inserted for file: {file}")
            else:
                logging.info(f"No matching log found for file: {file}")
        else:
            logging.info(f"Skipping file {file} due to missing UUID or timestamp.")

    conn.close()

def main():
    """Main function."""
    args = parse_arguments()
    config = load_config(args.config)
    settings = merge_config_with_args(args, config)

    setup_logging(settings["shredmatch_logfile"], settings["log_format"])
    create_database(settings["shredmatch_db_file"])

    logging.info("Starting ShredMatch process.")
    process_files(settings)
    logging.info("ShredMatch process completed.")

if __name__ == "__main__":
    main()
