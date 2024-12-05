#!/spindles/shred/.venv/shredsync/bin/python
# ShredMatch 2.2

import os
import re
import sys
import yaml
import argparse
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
import logging

# Version: ShredMatch 2.1

def load_config(config_file):
    """Load configuration from a YAML file."""
    try:
        with open(config_file, "r") as file:
            return yaml.safe_load(file)
    except FileNotFoundError:
        sys.exit(f"Configuration file not found: {config_file}")
    except yaml.YAMLError as e:
        sys.exit(f"Error reading configuration file: {e}")


def flatten_config(config):
    """Flatten a nested config dictionary into a single level."""
    flat_config = {}
    for section, options in config.items():
        if not isinstance(options, dict):
            continue
        for key, details in options.items():
            flat_config[key] = details
    return flat_config


def parse_arguments(config):
    """Parse command-line arguments based on the configuration."""
    parser = argparse.ArgumentParser(description="ShredMatch 2.1 Script")
    flat_config = flatten_config(config)

    # Dynamically add arguments
    for key, details in flat_config.items():
        parser.add_argument(
            details["command_argument"],
            default=details["value"],
            help=details["help"],
            dest=key,
            required=False
        )

    # Parse command-line arguments
    args = vars(parser.parse_args())

    # Merge with defaults
    for key, details in flat_config.items():
        if key not in args or args[key] is None:
            args[key] = details["value"]

    # Debug: Print parsed arguments
    logging.debug(f"Parsed arguments: {args}")

    return args


def setup_logging(log_file, log_dir_permission, log_file_permission, log_format):
    """Set up logging with the given parameters."""
    try:
        log_dir = os.path.dirname(log_file)
        os.makedirs(log_dir, mode=int(log_dir_permission, 8), exist_ok=True)

        # Ensure log file exists or create it
        if not os.path.exists(log_file):
            with open(log_file, 'a'):
                os.chmod(log_file, int(log_file_permission, 8))

        logging.basicConfig(
            filename=log_file,
            level=logging.DEBUG,  # Use DEBUG for detailed information
            format=log_format
        )
        logging.info("Logging setup complete.")
    except Exception as e:
        print(f"Failed to set up logging: {e}")
        sys.exit(1)



def setup_database(db_file, db_path):
    """Set up the database if it does not exist, or validate it if it does."""
    db_full_path = os.path.join(db_path, db_file)
    try:
        # Ensure the database directory exists
        os.makedirs(db_path, exist_ok=True)
        os.chmod(db_path, int("0755", 8))  # Set directory permissions if necessary

        # Create a new database if it doesn't exist
        if not os.path.exists(db_full_path):
            logging.info(f"Database not found at {db_full_path}. Creating a new one.")
            conn = sqlite3.connect(db_full_path)
            cursor = conn.cursor()
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS s_matches (
                unique_id INTEGER PRIMARY KEY AUTOINCREMENT,
                inserted_timestamp TIMESTAMP NOT NULL,
                shredded_timestamp TIMESTAMP NOT NULL,
                shredded_source TEXT,
                import_source TEXT,
                shred_log_source TEXT,
                shredded_uuid TEXT,
                import_uuid TEXT,
                import_folder TEXT,
                log_filename TEXT
            );
            """)
            conn.commit()
            conn.close()
        else:
            logging.info(f"Database found at {db_full_path}. Validating tables.")
        return db_full_path
    except Exception as e:
        logging.error(f"Error setting up database at {db_full_path}: {e}")
        raise


def find_files(base_path, recursive, file_patterns):
    """Find files matching given patterns in base_path."""
    matching_files = []
    if recursive.lower() == "yes":
        for root, _, files in os.walk(base_path):
            for file in files:
                if any(re.search(pattern, file, re.IGNORECASE) for pattern in file_patterns):
                    matching_files.append(os.path.join(root, file))
    else:
        for file in os.listdir(base_path):
            if any(re.search(pattern, file, re.IGNORECASE) for pattern in file_patterns):
                matching_files.append(os.path.join(base_path, file))
    return matching_files


def process_shredded_files(files):
    """Filter and process shredded files, extracting UUIDs and timestamps."""
    valid_files = {}
    photo_pattern = r"photo-(\d{12})-([A-Fa-f0-9-]{36})\.zip"
    for file in files:
        basename = os.path.basename(file)
        match = re.search(photo_pattern, basename, re.IGNORECASE)
        if match:
            raw_date, uuid = match.groups()
            try:
                timestamp = datetime.strptime(raw_date, "%Y%m%d%H%M")
                if uuid not in valid_files:
                    valid_files[uuid] = {
                        "date": timestamp,
                        "files_list": []
                    }
                valid_files[uuid]["files_list"].append(file)
            except ValueError:
                logging.warning(f"Invalid date format in file: {basename}")
    return valid_files


def find_logs(log_path, target_date, prior_days, after_days):
    """Find logs within the date range."""
    log_files = []
    log_pattern = r"com\.shredvideo\.ShredCentral (\d{4}-\d{2}-\d{2}) (\d{2}-\d{2})\.log"
    date_range = [target_date + timedelta(days=delta) for delta in range(-prior_days, after_days + 1)]
    for log in os.listdir(log_path):
        match = re.search(log_pattern, log, re.IGNORECASE)
        if match:
            log_date, _ = match.groups()
            log_date = datetime.strptime(log_date, "%Y-%m-%d")
            if log_date in date_range:
                log_files.append(log)
    return log_files


def process_logs(uuid, log_files, logs_path):
    """Search logs for the specified UUID and extract matching data."""
    match_pattern = rf"starting reading {uuid} .*(/import-[\w-]+)"
    import_folder, import_uuid, wholename = None, None, None
    for log_file in log_files:
        log_path = os.path.join(logs_path, log_file)
        with open(log_path, "r") as log:
            for line in log:
                match = re.search(match_pattern, line, re.IGNORECASE)
                if match:
                    import_folder = os.path.basename(match.group(1))
                    name_match = re.search(r"import-([\w-]+?)-", import_folder)
                    if name_match:
                        wholename = name_match.group(1)
                    uuid_match = re.search(r"([A-Fa-f0-9-]{36})$", import_folder)
                    if uuid_match:
                        import_uuid = uuid_match.group(1)
                    logging.info(f"Found match in {log_file}: {line.strip()}")
                    return log_file, import_folder, import_uuid, wholename
    return None, None, None, None


def main():
    script_base_path = Path(__file__).resolve().parent
    config_file = script_base_path / "config2.yaml"
    config = load_config(config_file)
    args = parse_arguments(config)
    shredmatch_logfile = config.get("shredmatch", {}).get("logfile", {}).get("value", "shredmatch.log")

    # Setup logging
    setup_logging(
        log_file=os.path.join(args["logpath"], shredmatch_logfile),
        log_dir_permission=args["log_dir_permission"],
        log_file_permission=args["log_file_permission"],
        log_format=args["log_format"]
    )
    print(f"Log path: {args['logpath']}")
    print(f"Log file: {shredmatch_logfile}")
    print(f"Database path: {args['db_path']}")
    print(f"Shredded source: {args['shredded_source']}")
    # Setup database
    db_path = setup_database(args["db_file"], args["db_path"])

    # Find shredded files
    shredded_files = find_files(
        base_path=args["shredded_source"],
        recursive=args["shredded_source-recursive"],
        file_patterns=[r".*\.zip$", r".*\.mp4$"]
    )
    logging.info(f"Found {len(shredded_files)} shredded files.")

    # Process shredded files
    valid_files = process_shredded_files(shredded_files)
    for uuid, details in valid_files.items():
        logging.info(f"Processing UUID: {uuid}")
        logging.info(f"Date: {details['date']}")

        # Find logs for this UUID
        logs = find_logs(
            log_path=args["shred_log_source"],
            target_date=details["date"],
            prior_days=int(args["shred_log_prior"]),
            after_days=int(args["shred_log_after"])
        )
        logging.info(f"Found {len(logs)} potential log files for UUID: {uuid}")

        # Process logs
        log_file, import_folder, import_uuid, wholename = process_logs(uuid, logs, args["shred_log_source"])
        if log_file:
            logging.info(f"Matched log file: {log_file}")
            logging.info(f"Import Folder: {import_folder}, Import UUID: {import_uuid}, Whole Name: {wholename}")
        else:
            logging.info(f"No matching log file found for UUID: {uuid}")


if __name__ == "__main__":
    main()
