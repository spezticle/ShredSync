#!/opt/ShredBackupSync/venv/bin/python3

import os
import subprocess
import yaml
import sys
import logging
from datetime import datetime, timedelta

# Load configuration from YAML file
CONFIG_FILE = "/opt/ShredBackupSync/config.yaml"

def load_config(config_file):
    with open(config_file, "r") as file:
        return yaml.safe_load(file)

# Initialize logging
def initialize_logging(log_path):
    log_file = os.path.join(log_path, f"{datetime.now().strftime('%Y%m%d-%H%M%S')}.log")
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    return log_file

def get_remote_folders(remote_path, ssh_user, identity_file, days_threshold):
    logging.info("Retrieving remote folder list...")
    remote_host, remote_dir = remote_path.split(':')
    command = [
        "ssh", "-i", identity_file, f"{ssh_user}@{remote_host}",
        f"gfind {remote_dir} -mindepth 1 -maxdepth 1 -type d -printf '%P|%T@\n'"
    ]
    try:
        result = subprocess.run(command, capture_output=True, text=True, check=True)
    except subprocess.CalledProcessError as e:
        logging.error(f"Error retrieving remote folder list: {e.stderr.strip()}")
        raise RuntimeError("Failed to retrieve remote folders.") from e

    raw_output = result.stdout.strip()
    logging.debug(f"Raw output from find command: {raw_output}")
    print("DEBUG: Raw output from find command:")
    print(raw_output)

    now = datetime.now().timestamp()
    folders = []

    for line in raw_output.splitlines():
        line = line.strip()
        if not line:
            logging.debug("Skipping empty line in output.")
            continue

        if '|' not in line:
            logging.warning(f"Unexpected line format: {line}")
            continue

        try:
            name, mod_time = line.split('|', 1)
            mod_time = float(mod_time)
            if (now - mod_time) > (days_threshold * 86400):  # Older than threshold
                folders.append(name)
        except ValueError as e:
            logging.warning(f"Error parsing line: {line}. Exception: {str(e)}")
            continue

    logging.info(f"Retrieved {len(folders)} folders.")
    return folders
    
    
# Ensure destination path structure exists
def ensure_path_structure(local_path, folder):
    components = folder.split('-')
    if len(components) < 4:
        raise ValueError("Folder name does not match the expected pattern.")

    _, name, timestamp, _ = components
    date = datetime.strptime(timestamp, "%Y%m%d%H%M")
    year = date.strftime("%Y")
    month = date.strftime("%m-%B")
    day = date.strftime("%d-%A")

    destination = os.path.join(local_path, year, month, day, name, folder)
    os.makedirs(destination, exist_ok=True)
    return destination

# Rsync folders
def rsync_folder(remote_path, folder, local_path, ssh_user, identity_file):
    remote_folder = f"{remote_path}/{folder}"
    destination_folder = ensure_path_structure(local_path, folder)
    command = [
        "rsync", "-avz", "-e", f"ssh -i {identity_file}",
        f"{ssh_user}@{remote_folder}", destination_folder
    ]
    logging.info(f"Syncing folder: {remote_folder} -> {destination_folder}")
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode != 0:
        logging.error(f"Error syncing folder {folder}: {result.stderr.strip()}")
        raise RuntimeError(f"Rsync failed for {folder}.")
    logging.info(f"Successfully synced folder: {folder}")

# Delete remote folders
def delete_remote_folder(remote_path, folder, ssh_user, identity_file):
    remote_folder = f"{remote_path}/{folder}"
    command = ["ssh", "-i", identity_file, f"{ssh_user}@{remote_path.split(':')[0]}", f"rm -rf {remote_folder}"]
    logging.info(f"Deleting remote folder: {remote_folder}")
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode != 0:
        logging.error(f"Error deleting folder {folder}: {result.stderr.strip()}")
        raise RuntimeError(f"Failed to delete {folder}.")
    logging.info(f"Successfully deleted folder: {folder}")

# Main function
def main():
    config = load_config(CONFIG_FILE)
    remote_path = config["remote_path"]
    local_path = config["local_path"]
    ssh_user = config["ssh_user"]
    identity_file = config["identity_file"]
    days_threshold = config["days_threshold"]
    log_path = config["log_path"]
    
    log_file = initialize_logging(log_path)
    logging.info("Starting ShredBackupSync script.")

    # Parse command-line arguments
    if len(sys.argv) < 2:
        logging.error("No action provided. Use 'list', 'sync delete', or 'sync nodelete'.")
        sys.exit("Usage: script.py list|sync delete|sync nodelete")

    action = sys.argv[1].lower()
    delete = "delete" in sys.argv

    try:
        # Retrieve remote folders
        folders = get_remote_folders(remote_path, ssh_user, identity_file, days_threshold)
        logging.info(f"Folders to process: {folders}")

        if action == "list":
            print("Folders to process:")
            for folder in folders:
                print(folder)
            logging.info("Listed folders only.")
            return

        # Sync and optionally delete
        for folder in folders:
            rsync_folder(remote_path, folder, local_path, ssh_user, identity_file)
            if delete:
                delete_remote_folder(remote_path, folder, ssh_user, identity_file)
        
        logging.info("All operations completed successfully.")
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        print(f"Error: {str(e)}")
    finally:
        print(f"Log written to {log_file}")

if __name__ == "__main__":
    main()
