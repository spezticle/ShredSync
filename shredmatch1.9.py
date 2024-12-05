#!/spindles/shred/.venv/shredsync/bin/python
# Shred Match 1.9#

import os
import re
import argparse
import yaml
from datetime import datetime, timedelta
from collections import Counter


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
    for delta in [-2, -1, 0, +1]:
        target_date = timestamp + timedelta(days=delta)
        for log_datetime, log_file in potential_logs:
            if log_datetime.date() == target_date.date():
                result_logs.append((delta, log_file))
                break
    return result_logs


def search_customer_info_in_logs(uuid, log_files, logs_path):
    """Search for UUID and extract customer information from logs."""
    customer_info = None
    log_pattern = rf"starting reading {uuid}.*"
    customer_pattern = r"updating customer .*?, (\w+ \w+) (\S+@\S+)"

    for day_offset, log_file in log_files:
        log_path = os.path.join(logs_path, log_file)
        if not os.path.isfile(log_path):
            continue

        with open(log_path, "r") as log:
            for line in log:
                if re.search(log_pattern, line, re.IGNORECASE):
                    match = re.search(customer_pattern, line, re.IGNORECASE)
                    if match:
                        name, email = match.groups()
                        customer_info = {
                            "proper_name": name,
                            "email": email,
                            "log_file": log_file,
                            "day_offset": day_offset,
                        }
                        return customer_info
    return None


def display_results(results, day_offset_counts):
    """Display matched results in the requested format."""
    print("\nMatched Results:")
    for group in results:
        print(f"\n{group['photo_file']}")
        print("    associated files:")
        for file in group['related_files']:
            print(f"        {file}")
        if group['log_files']:
            for day_offset, log_file in group['log_files']:
                print(f"            log file ({'+' if day_offset > 0 else ''}{day_offset} days): {log_file}")
            print(f"            date: {group['timestamp'].strftime('%Y-%m-%d %H:%M:%S')}")
        if group['import_folder'] and group['name']:
            print(f"    import folder: {group['import_folder']}")
            print(f"    name: {group['name']}")
        else:
            print("    import folder: None")
            print("    name: None")
        if group['customer_info']:
            print(f"    proper name: {group['customer_info']['proper_name']}")
            print(f"    customer_email: {group['customer_info']['email']}")
            print(f"    log_file: {group['customer_info']['log_file']}")
            print(f"    day_track: {group['customer_info']['day_offset']}")
        else:
            print("    customer info: None")
    print("\nDay Offset Counts:")
    for offset, count in day_offset_counts.items():
        print(f"    {offset:+}: {count}")


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
    day_offset_counts = Counter()

    for file in source_files:
        if "photo-" in file.lower():
            uuid, timestamp = extract_uuid_and_date(file)
            if uuid and timestamp:
                related_files = find_related_files(source_files, uuid)
                log_files = find_corresponding_log(shredlogs, timestamp)
                import_folder, name = search_uuid_in_logs(uuid, log_files, shredlogs)
                customer_info = search_customer_info_in_logs(uuid, log_files, shredlogs)

                for offset, _ in log_files:
                    day_offset_counts[offset] += 1

                results.append({
                    "photo_file": file,
                    "related_files": related_files,
                    "log_files": log_files,
                    "timestamp": timestamp,
                    "import_folder": import_folder,
                    "name": name,
                    "customer_info": customer_info,
                })

    display_results(results, day_offset_counts)


if __name__ == "__main__":
    main()
