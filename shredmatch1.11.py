#!/spindles/shred/.venv/shredsync/bin/python
# Shred Match 1.11#

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


def search_uuid_in_logs(uuid, log_files, logs_path):
    """Search for UUID in the specified log files."""
    import_folder = None
    name = None
    matching_logs = []
    pattern = r"starting reading " + re.escape(uuid) + r" .*(/import-[\w-]+)"

    for log_file in log_files:
        log_path = os.path.join(logs_path, log_file)
        if not os.path.isfile(log_path):
            continue

        with open(log_path, "r") as log:
            for line in log:
                match = re.search(pattern, line, re.IGNORECASE)
                if match:
                    import_folder = os.path.basename(match.group(1))
                    name_match = re.search(r"import-([\w-]+?)-", import_folder)
                    name = name_match.group(1) if name_match else None
                    matching_logs.append(log_file)
                    return import_folder, name, matching_logs

    return None, None, matching_logs


def search_customer_info(name, matching_logs, logs_path):
    """Search for proper name and email using name in matching logs."""
    proper_name = None
    customer_email = None
    pattern = r"(new customer|updating customer).*created: .*?, (.+?) ([\w.%+-]+@[a-zA-Z.-]+)"

    for log_file in matching_logs:
        log_path = os.path.join(logs_path, log_file)
        if not os.path.isfile(log_path):
            continue

        with open(log_path, "r") as log:
            for line in log:
                match = re.search(pattern, line, re.IGNORECASE)
                if match:
                    full_name, email = match.group(2), match.group(3)
                    if re.search(name, full_name, re.IGNORECASE):
                        names = full_name.split()
                        proper_name = ' '.join(names[:-1])
                        customer_email = email
                        return proper_name, customer_email

    return proper_name, customer_email


def display_results(results):
    """Display matched results in the requested format."""
    day_counts = {"-2": 0, "-1": 0, "0": 0, "+1": 0}

    print("\nMatched Results:")
    for group in results:
        print(f"\n{group['photo_file']}")
        print("    associated files:")
        for file in group['related_files']:
            print(f"        {file}")
        if group['log_files']:
            for log_file, delta in zip(group['log_files'], [-2, -1, 0, 1]):
                print(f"            log file ({'+' if delta > 0 else ''}{delta} days): {log_file}")
            print(f"            date: {group['timestamp'].strftime('%Y-%m-%d %H:%M:%S')}")
        if group['import_folder'] and group['name']:
            print(f"    import folder: {group['import_folder']}")
            print(f"    name: {group['name']}")
        if group['customer_info']:
            print(f"    proper name: {group['customer_info']['proper_name']}")
            print(f"    customer_email: {group['customer_info']['customer_email']}")
        else:
            print("    customer info: None")
    print("\nTotal counts:")
    for day, count in day_counts.items():
        print(f"    {day}: {count}")
    print("\n")


def main():
    args = parse_arguments()
    config = load_config(args.config)

    source = args.source or config.get("source")
    source_recursive = args.source_recursive.lower() == "yes"
    shredlogs = args.shredlogs or config.get("shredlogs")
    destination = args.destination or config.get("destination")

    source_files = get_files_from_source(source, source_recursive)
    results = []
    for file in source_files:
        if "photo-" in file.lower():
            uuid, timestamp = extract_uuid_and_date(file)
            if uuid and timestamp:
                related_files = find_related_files(source_files, uuid)
                log_files = find_corresponding_log(shredlogs, timestamp)
                import_folder, name, matching_logs = search_uuid_in_logs(uuid, log_files, shredlogs)
                if matching_logs:
                    proper_name, customer_email = search_customer_info(name, matching_logs, shredlogs)
                else:
                    proper_name = customer_email = None

                results.append({
                    "photo_file": file,
                    "related_files": related_files,
                    "log_files": log_files,
                    "timestamp": timestamp,
                    "import_folder": import_folder,
                    "name": name,
                    "customer_info": {
                        "proper_name": proper_name,
                        "customer_email": customer_email,
                    },
                })

    display_results(results)


if __name__ == "__main__":
    main()