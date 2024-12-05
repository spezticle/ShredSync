#!/usr/bin/env python3
import logging
import os
import re
import argparse
import yaml
from datetime import datetime, timedelta


def load_config(config_file):
    """Load configuration from a YAML file."""
    try:
        with open(config_file, "r") as file:
            return yaml.safe_load(file)
    except FileNotFoundError:
        return {}
    except yaml.YAMLError as e:
        sys.exit(f"Error reading configuration file: {e}")

def setup_logging(log_path, log_file):
    """
    Initialize logging to file and console.
    """
    # Ensure the log directory exists
    os.makedirs(log_path, exist_ok=True)
    log_file_path = os.path.join(log_path, log_file)

    # Set up basic logging configuration
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file_path),  # Log to file
            logging.StreamHandler(sys.stdout),  # Log to console
        ]
    )
    logging.info(f"Logging initialized. Log file: {log_file_path}")


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
    if recursive:
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

    # Find the exact match and range (-2 to +1 days)
    result_logs = []
    for delta in [-2, -1, 0, 1]:
        target_date = timestamp + timedelta(days=delta)
        for log_datetime, log_file in potential_logs:
            if log_datetime.date() == target_date.date():
                result_logs.append(log_file)
                break  # Ensure only one match per day in the range

    return result_logs


def extract_import_folder_and_name(logs_path, uuid):
    """Extract the import folder and name from the logs based on UUID."""
    log_pattern = r"starting reading " + re.escape(uuid) + r" .*(/import-[\w-]+)"
    import_folder = None
    name = None

    # Log the number of UUIDs found
    logging.info(f"Searching for UUID: {uuid}")
    print(f"Searching for UUID: {uuid}")

    # Iterate through log files
    log_files = os.listdir(logs_path)
    logging.info(f"Log files found: {len(log_files)}")
    print(f"Log files found: {len(log_files)}")

    for log_file in log_files:
        log_path = os.path.join(logs_path, log_file)
        if not os.path.isfile(log_path):  # Ensure it's a regular file
            continue

        # Log current log file being processed
        logging.info(f"Looking in log file: {log_file}")
        print(f"Looking in log file: {log_file}")

        with open(log_path, "r") as log:
            for line in log:
                match = re.search(log_pattern, line, re.IGNORECASE)
                if match:
                    # Log the matching line
                    logging.info(f"Found matching line: {line.strip()}")
                    print(f"Found matching line: {line.strip()}")

                    import_folder = os.path.basename(match.group(1))
                    name_match = re.search(r"import-([\w-]+?)-", import_folder)
                    name = name_match.group(1) if name_match else None

                    # Log the extracted folder and name
                    logging.info(f"Found folder: {import_folder}")
                    print(f"Found folder: {import_folder}")
                    logging.info(f"Extracted name: {name}")
                    print(f"Extracted name: {name}")

                    return import_folder, name

    # Log when no match is found
    logging.warning(f"No matching log entry found for UUID: {uuid}")
    print(f"No matching log entry found for UUID: {uuid}")
    return None, None


def display_results(results, source_path, shredlogs_path):
    """Display matched results in the requested format."""
    print("\nMatched Results:")
    for group in results:
        print(f"\n{group['photo_file']}")
        print("    associated files:")
        for file in group['related_files']:
            print(f"        {file}")
        if group['log_files']:
            for log_file, delta in zip(group['log_files'], [-2, -1, 0, 1]):
                date_offset = f"            log file ({'+' if delta > 0 else ''}{delta} days): {log_file}"
                print(date_offset)
            print(f"            date: {group['timestamp'].strftime('%Y-%m-%d %H:%M:%S')}")
        if group['import_folder'] and group['name']:
            print(f"    import folder: {group['import_folder']}")
            print(f"    name: {group['name']}")
        else:
            print("    import folder: None")
            print("    name: None")
    print("\n")


def main():
    args = parse_arguments()
    config = load_config(args.config)
    log_path = "/spindles/shred/log"
    log_file = "shredmatch1.5.log"
    logging.info(f"Logging to: {os.path.join(log_path, log_file)}")

    # Merge arguments with config file (arguments take precedence)
    source = args.source or config.get("source")
    source_recursive = args.source_recursive.lower() == "yes"
    shredlogs = args.shredlogs or config.get("shredlogs")
    destination = args.destination or config.get("destination")

    if not (source and shredlogs and destination):
        sys.exit("Error: Missing required parameters (source, shredlogs, destination).")

    # Collect files from source
    source_files = get_files_from_source(source, source_recursive)

    # Match photo files and group related data
    results = []
    for file in source_files:
        if "photo-" in file.lower():
            uuid, timestamp = extract_uuid_and_date(file)
            if uuid and timestamp:
                related_files = find_related_files(source_files, uuid)
                log_files = find_corresponding_log(shredlogs, timestamp)
                import_folder, name = extract_import_folder_and_name(shredlogs, uuid)
                results.append({
                    "photo_file": file,
                    "related_files": related_files,
                    "log_files": log_files,
                    "timestamp": timestamp,
                    "import_folder": import_folder,
                    "name": name,
                })

    # Display results
    display_results(results, source, shredlogs)


if __name__ == "__main__":
    main()