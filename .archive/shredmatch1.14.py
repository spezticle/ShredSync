#!/spindles/shred/.venv/shredsync/bin/python
# Shred Match 1.14 - Dynamic Name Extraction

import os
import re
import argparse
import yaml
from datetime import datetime, timedelta
from pathlib import Path


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


def get_files_from_source(source, recursive):
    """Get relevant files (.mp4, .zip, .jpg) from the source folder."""
    file_patterns = [r".*\.mp4$", r".*\.zip$", r".*\.jpg$"]
    files = []
    if recursive:
        for root, _, filenames in os.walk(source):
            for file in filenames:
                if any(re.search(pattern, file, re.IGNORECASE) for pattern in file_patterns):
                    files.append(file)
    else:
        for file in os.listdir(source):
            if any(re.search(pattern, file, re.IGNORECASE) for pattern in file_patterns):
                files.append(file)
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


def extract_name_from_import_folder(import_folder):
    """Extract the name from the import folder format."""
    match = re.search(r"import-([\w-]+?)-", import_folder, re.IGNORECASE)
    return match.group(1) if match else None


def search_customer_info(name, matching_logs, logs_path):
    """Search for proper name and email using the provided name in matching logs."""
    proper_name = None
    customer_email = None
    pattern = r"updating customer.*created: .*?, (.+?) ([\w.%+-]+@[a-zA-Z.-]+)"

    print(f"DEBUG: Starting search for name: {name}")
    for log_file in matching_logs:
        log_path = os.path.join(logs_path, log_file)
        if not os.path.isfile(log_path):
            continue

        print(f"Processing log file: {log_file}")
        with open(log_path, "r") as log:
            for line in log:
                match = re.search(pattern, line, re.IGNORECASE)
                if match:
                    name_chunk = match.group(1)
                    email = match.group(2)

                    normalized_input_name = name.lower()
                    normalized_name_chunk = name_chunk.lower()

                    name_parts = normalized_name_chunk.split()
                    first_name = name_parts[0] if name_parts else ""
                    last_name = " ".join(name_parts[1:]) if len(name_parts) > 1 else ""

                    print(f"Comparing input name: {normalized_input_name} to proper name: {normalized_name_chunk}")
                    print(f"Extracted First name: {first_name}, Last name: {last_name}")

                    if first_name in normalized_input_name and last_name in normalized_input_name:
                        proper_name = name_chunk
                        customer_email = email
                        print(f"Match found! Proper name: {proper_name}, Email: {customer_email}")
                        return proper_name, customer_email
                    else:
                        print(f"No match: {normalized_input_name} does not match {normalized_name_chunk}")

    return proper_name, customer_email


def display_results(results):
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
        if group['proper_name'] and group['customer_email']:
            print(f"    proper name: {group['proper_name']}")
            print(f"    customer email: {group['customer_email']}")
        else:
            print("    proper name: None")
            print("    customer email: None")
    print("\n")


def main():
    args = parse_arguments()
    config = load_config(args.config)

    source = args.source or config.get("source")
    source_recursive = args.source_recursive.lower() == "yes"
    shredlogs = args.shredlogs or config.get("shredlogs")
    destination = args.destination or config.get("destination")

    if not (source and shredlogs and destination):
        sys.exit("Error: Missing required parameters (source, shredlogs, destination).")

    source_files = get_files_from_source(source, source_recursive)
    results = []
    for file in source_files:
        if "photo-" in file.lower():
            uuid, timestamp = extract_uuid_and_date(file)
            if uuid and timestamp:
                related_files = find_related_files(source_files, uuid)
                log_files = find_corresponding_log(shredlogs, timestamp)
                import_folder = f"import-{uuid}"  # Example import folder naming
                name = extract_name_from_import_folder(import_folder)
                if not name:
                    print(f"ERROR: Unable to extract name from import folder: {import_folder}")
                    continue

                print(f"DEBUG: Name before search_customer_info call: {name}")
                proper_name, customer_email = search_customer_info(name, log_files, shredlogs)

                results.append({
                    "photo_file": file,
                    "related_files": related_files,
                    "log_files": log_files,
                    "timestamp": timestamp,
                    "import_folder": import_folder,
                    "name": name,
                    "proper_name": proper_name,
                    "customer_email": customer_email,
                })

    display_results(results)


if __name__ == "__main__":
    main()