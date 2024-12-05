#!/spindles/shred/.venv/shredsync/bin/python
# ShredMatch 2.7

import os
import re
import sys
import yaml
import argparse
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
import shutil
import logging

# Version: ShredMatch 2.7

# ---- Helper Functions ----

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
        if isinstance(options, dict):
            for key, details in options.items():
                flat_config[key] = details
    return flat_config


def parse_arguments(config):
    """Parse command-line arguments based on the configuration."""
    parser = argparse.ArgumentParser(description="ShredMatch 2.7 Script")
    flat_config = flatten_config(config)

    for key, details in flat_config.items():
        parser.add_argument(
            details["command_argument"],
            default=details["value"],
            help=details["help"],
            dest=key,
            required=False
        )

    args = vars(parser.parse_args())
    return args


def merge_config_and_args(config, args):
    """Merge command-line arguments into the configuration, with arguments taking precedence."""
    flat_config = flatten_config(config)
    merged = {key: details["value"] for key, details in flat_config.items()}

    for key, value in args.items():
        if value is not None:
            merged[key] = value

    return merged


def setup_logging(log_file, log_dir_permission, log_file_permission, log_format):
    """Set up logging with the given parameters."""
    log_dir = os.path.dirname(log_file)
    os.makedirs(log_dir, mode=int(log_dir_permission, 8), exist_ok=True)
    if not os.path.exists(log_file):
        with open(log_file, 'a'):
            os.chmod(log_file, int(log_file_permission, 8))
    logging.basicConfig(filename=log_file, level=logging.INFO, format=log_format)
    logging.info("Logging setup complete.")


def setup_database(db_file, db_path):
    """Set up the database if it does not exist, or validate it if it does."""
    db_full_path = os.path.join(db_path, db_file)
    try:
        os.makedirs(db_path, exist_ok=True)
        os.chmod(db_path, int("0755", 8))

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
            logging.warning(f"Invalid date format in filename: {filename}")
    return None, None


def find_related_files(source_files, uuid):
    """Find all files related to the given UUID."""
    return [f for f in source_files if re.search(uuid, f, re.IGNORECASE)]


def format_date_to_path(timestamp):
    """Generate a directory path from a timestamp."""
    year = timestamp.strftime("%Y")
    month = timestamp.strftime("%m-%B")
    day = timestamp.strftime("%d-%A")
    return os.path.join(year, month, day)


def format_file_description(filename, uuid):
    """Format the file description for renaming."""
    pattern = rf"{uuid}-(.+?)\.(.+)$"
    match = re.search(pattern, filename, re.IGNORECASE)
    if match:
        description, extension = match.groups()
        description = description.replace("_", " ").title()
        return description, extension
    return None, None


def copy_files_to_destination(files, customer_details, timestamp, destination):
    """Copy files to the destination directory with the new structure."""
    first_name = customer_details.get("first_name", "Unknown").title()
    last_name = customer_details.get("last_name", "Unknown").title()
    full_name = f"{first_name} {last_name}"

    date_path = format_date_to_path(timestamp)
    base_path = os.path.join(destination, date_path, full_name)

    os.makedirs(base_path, exist_ok=True)

    for file in files:
        description, extension = format_file_description(file, customer_details["uuid"])
        if description:
            new_filename = f"{full_name} - {description}.{extension}"
            dest_path = os.path.join(base_path, new_filename)
            shutil.copy2(file, dest_path)
            logging.info(f"Copied {file} to {dest_path}")
        else:
            logging.warning(f"Could not parse description for file: {file}")


def find_corresponding_log(logs_path, timestamp, shred_log_prior, shred_log_after):
    """Find log files within the specified date range."""
    log_pattern = r"com\.shredvideo\.ShredCentral (\d{4}-\d{2}-\d{2}) (\d{2}-\d{2})\.log"
    potential_logs = []
    for log_file in os.listdir(logs_path):
        match = re.search(log_pattern, log_file, re.IGNORECASE)
        if match:
            log_date, log_time = match.groups()
            log_datetime = datetime.strptime(f"{log_date} {log_time.replace('-', ':')}", "%Y-%m-%d %H:%M")
            potential_logs.append((log_datetime, log_file))

    result_logs = []
    date_range = [timestamp + timedelta(days=delta) for delta in range(-shred_log_prior, shred_log_after + 1)]
    for target_date in date_range:
        for log_datetime, log_file in potential_logs:
            if log_datetime.date() == target_date.date():
                result_logs.append(log_file)
                break

    return result_logs


def search_uuid_in_logs(uuid, log_files, logs_path):
    """Search for UUID in the specified log files."""
    import_folder, name = None, None
    pattern = rf"starting reading {uuid} .*(/import-[\w-]+)"
    for log_file in log_files:
        log_path = os.path.join(logs_path, log_file)
        with open(log_path, "r") as log:
            for line in log:
                match = re.search(pattern, line, re.IGNORECASE)
                if match:
                    import_folder = os.path.basename(match.group(1))
                    name_match = re.search(r"import-([\w-]+?)-", import_folder)
                    name = name_match.group(1) if name_match else None
                    return log_file, import_folder, name
    return None, None, None


def extract_customer_details(name, log_path):
    """Extract customer details from the log file based on the name."""
    pattern = r"uploaded customer packages customer \d+, created: .+, (\w+) (.+) ([\w\.\-]+@[\w\.\-]+) (https?://[\w\./\-]+)"
    try:
        with open(log_path, "r") as log_file:
            for line in log_file:
                match = re.search(pattern, line)
                if match:
                    first_name, full_last_name, email, url = match.groups()
                    full_last_name = full_last_name.strip()
                    if first_name.lower() in name.lower() and full_last_name.lower() in name.lower():
                        return {
                            "first_name": first_name,
                            "last_name": full_last_name,
                            "email": email,
                            "url": url
                        }
    except FileNotFoundError:
        logging.error(f"Log file not found: {log_path}")
    except Exception as e:
        logging.error(f"Error processing log file {log_path}: {e}")
    return None


# ---- Main Function ----

def main():
    script_base_path = Path(__file__).resolve().parent
    config_file = script_base_path / "config2.yaml"

    config = load_config(config_file)
    args = parse_arguments(config)
    script_options = merge_config_and_args(config, args)

    logpath = script_options["logpath"]
    logfile = script_options["logfile"]
    shredmatch_logfile = os.path.join(logpath, logfile)
    setup_logging(
        log_file=shredmatch_logfile,
        log_dir_permission=script_options["log_dir_permission"],
        log_file_permission=script_options["log_file_permission"],
        log_format=script_options["log_format"]
    )

    db_path = setup_database(script_options["db_file"], script_options["db_path"])

    shredded_files = find_files(
        script_options["shredded_source"],
        script_options["shredded_source_recursive"],
        [r".*\.zip$", r".*\.mp4$", r".*\.jpg$"]
    )

    valid_files = {}
    for file in shredded_files:
        uuid, timestamp = extract_uuid_and_date(file)
        if uuid and timestamp:
            if uuid not in valid_files:
                valid_files[uuid] = {"timestamp": timestamp, "files": []}
            valid_files[uuid]["files"].append(file)

    for uuid, data in valid_files.items():
        timestamp = data["timestamp"]
        files = find_related_files(shredded_files, uuid)
        log_files = find_corresponding_log(
            script_options["shred_log_source"],
            timestamp,
            int(script_options["shred_log_prior"]),
            int(script_options["shred_log_after"])
        )
        matching_log, import_folder, name = search_uuid_in_logs(uuid, log_files, script_options["shred_log_source"])
        customer_details = None
        if matching_log and name:
            customer_details = extract_customer_details(name, os.path.join(script_options["shred_log_source"], matching_log))

        if customer_details:
            customer_details["uuid"] = uuid
            logging.info(f"UUID: {uuid}, Files: {files}, Log Files: [{matching_log}], "
                         f"Import Folder: {import_folder}, Name: {name}, Customer Details: {customer_details}")

            copy_files_to_destination(
                files=files,
                customer_details=customer_details,
                timestamp=timestamp,
                destination=script_options["destination"]
            )


if __name__ == "__main__":
    main()