#!/spindles/shred/.venv/shredsync/bin/python3
import os
import sys
import logging
import shutil
import yaml
import re
from datetime import datetime
from time import time

CONFIG_FILE = "config.yaml"

def load_config(config_file):
    with open(config_file, "r") as file:
        return yaml.safe_load(file)

def initialize_logging(log_path, log_file, log_format):
    os.makedirs(log_path, exist_ok=True)
    log_file = os.path.join(log_path, log_file)
    logging.basicConfig(
        filename=log_file,
        level=logging.DEBUG,
        format=log_format
    )
    return log_file

def load_history(history_file):
    if not os.path.exists(history_file):
        return set()
    with open(history_file, "r") as file:
        return set(line.strip() for line in file)

def update_history(history_file, folder):
    try:
        with open(history_file, "a") as file:
            file.write(f"{folder}\n")
        logging.info(f"Updated history log with folder: {folder}")
    except Exception as e:
        logging.error(f"Failed to update history log for folder {folder}: {e}")
        raise

def validate_folder_name(folder_name):
    pattern = r"^import-[a-zA-Z0-9_ -]+-\d{12}-[a-fA-F0-9-]{36}$"
    return re.match(pattern, folder_name) is not None

def ensure_path_structure(local_path, folder):
    pattern = r"^(import-[a-zA-Z0-9_ -]+)-(\d{12})-([a-fA-F0-9-]{36})$"
    match = re.match(pattern, folder)
    if not match:
        raise ValueError(f"Folder name does not match the expected pattern: {folder}")

    full_name, timestamp, uuid = match.groups()
    date = datetime.strptime(timestamp, "%Y%m%d%H%M")
    year = date.strftime("%Y")
    month = date.strftime("%m-%B")
    day = date.strftime("%d-%A")
    name = full_name[len("import-"):]  # Strip "import-" prefix from the name

    destination = os.path.join(local_path, year, month, day, name, folder)
    os.makedirs(destination, exist_ok=True)
    return destination

def copy_folder(source_folder, destination_folder):
    try:
        shutil.copytree(source_folder, destination_folder, dirs_exist_ok=True)
        logging.info(f"Copied folder: {source_folder} -> {destination_folder}")
    except Exception as e:
        logging.error(f"Failed to copy folder {source_folder} to {destination_folder}: {e}")
        raise

def move_folder(source_folder, destination_folder):
    try:
        shutil.move(source_folder, destination_folder)
        logging.info(f"Moved folder: {source_folder} -> {destination_folder}")
    except Exception as e:
        logging.error(f"Failed to move folder {source_folder} to {destination_folder}: {e}")
        raise

def set_global_umask(umask_value):
    os.umask(int(umask_value, 8))
    logging.info(f"Set global umask to {umask_value}")

def main():
    config = load_config(CONFIG_FILE)

    # Initialize logging
    log_path = config["log_path"]
    log_file = config.get("log_file_format", "shredsync.log")
    log_format = config.get("log_format", "%(asctime)s - %(levelname)s - %(message)s")
    initialize_logging(log_path, log_file, log_format)

    # Configuration parameters
    set_global_umask(config.get("umask", "022"))
    remote_path = config["remote_path"]
    local_path = config["local_path"]
    history_file = config["history_file"]
    processed_folders = load_history(history_file)

    # Parse command-line arguments
    if len(sys.argv) < 2:
        logging.error("No action provided. Use '--movelocal' or default.")
        sys.exit("Usage: script.py [--movelocal]")

    movelocal = "--movelocal" in sys.argv
    action_type = "moving" if movelocal else "copying"
    logging.info(f"Starting ShredBackupSync script with {action_type} mode.")

    # Start processing folders
    folders = [
        folder for folder in os.listdir(remote_path)
        if os.path.isdir(os.path.join(remote_path, folder))
    ]

    total_folders = len(folders)
    logging.info(f"Total folders found: {total_folders}")
    start_time = time()

    for i, folder in enumerate(folders, 1):
        if folder in processed_folders:
            logging.info(f"Skipping already processed folder: {folder}")
            continue

        if not validate_folder_name(folder):
            logging.warning(f"Skipping invalid folder: {folder}")
            continue

        source_folder = os.path.join(remote_path, folder)
        destination_folder = ensure_path_structure(local_path, folder)

        logging.info(f"Processing folder {i}/{total_folders}: {folder}")
        print(f"Processing folder {i}/{total_folders}: {folder}")

        try:
            if movelocal:
                move_folder(source_folder, destination_folder)
            else:
                copy_folder(source_folder, destination_folder)
            update_history(history_file, folder)
        except Exception as e:
            logging.error(f"Error processing folder {folder}: {e}")
            continue

    elapsed_time = time() - start_time
    logging.info(f"All operations completed in {elapsed_time:.2f} seconds.")
    print(f"All operations completed in {elapsed_time:.2f} seconds.")

if __name__ == "__main__":
    main()
