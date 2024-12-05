#!/spindles/shred/.venv/shredsync/bin/python3
import os
import sys
import logging
import shutil
import yaml
import argparse
import subprocess
from datetime import datetime
from time import time
import re

# Determine the directory of this script
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
CONFIG_FILE = os.path.join(SCRIPT_DIR, "config.yaml")


def load_config(config_file):
    """
    Load configuration from the YAML file.
    """
    try:
        with open(config_file, "r") as file:
            return yaml.safe_load(file)
    except FileNotFoundError:
        sys.exit(f"Configuration file not found: {config_file}")
    except yaml.YAMLError as e:
        sys.exit(f"Error reading configuration file: {e}")


def initialize_logging(log_path, log_file, log_format):
    """
    Initialize logging.
    """
    os.makedirs(log_path, exist_ok=True)
    log_file_path = os.path.join(log_path, log_file)
    logging.basicConfig(
        filename=log_file_path,
        level=logging.DEBUG,
        format=log_format
    )
    logging.getLogger().addHandler(logging.StreamHandler())  # Log to console as well
    return log_file_path


def load_history(history_file):
    """
    Load history of processed folders.
    """
    if not os.path.exists(history_file):
        return set()
    with open(history_file, "r") as file:
        return set(line.strip() for line in file)


def update_history(history_file, folder):
    """
    Update history file with the processed folder.
    """
    try:
        with open(history_file, "a") as file:
            file.write(f"{folder}\n")
        logging.info(f"Updated history log with folder: {folder}")
    except Exception as e:
        logging.error(f"Failed to update history log: {e}")


def validate_folder_name(folder_name):
    """
    Validate folder name against the expected format.
    """
    pattern = r"^import-[a-zA-Z0-9_ -]+-\d{12}-[a-fA-F0-9-]{36}$"
    return re.match(pattern, folder_name) is not None


def ensure_path_structure(local_path, folder):
    """
    Ensure destination path structure based on folder name.
    """
    pattern = r"^(import-[a-zA-Z0-9_ -]+)-(\d{12})-([a-fA-F0-9-]{36})$"
    match = re.match(pattern, folder)
    if not match:
        raise ValueError(f"Invalid folder name format: {folder}")

    full_name, timestamp, uuid = match.groups()
    date = datetime.strptime(timestamp, "%Y%m%d%H%M")
    destination = os.path.join(
        local_path,
        date.strftime("%Y"),
        date.strftime("%m-%B"),
        date.strftime("%d-%A"),
        full_name[len("import-"):],
        folder
    )
    os.makedirs(destination, exist_ok=True)
    return destination


def move_folder(source_folder, destination_folder):
    """
    Move the folder to the destination.
    """
    try:
        shutil.move(source_folder, destination_folder)
        logging.info(f"Moved folder: {source_folder} -> {destination_folder}")
    except Exception as e:
        logging.error(f"Failed to move folder: {e}")
        raise


def rsync_folder(source_folder, destination_folder, rsync_options, days_threshold=None):
    """
    Rsync the folder and optionally remove old source files based on age.
    """
    try:
        command = ["rsync"] + rsync_options.split() + [source_folder, destination_folder]
        logging.info(f"Executing: {' '.join(command)}")

        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        for line in process.stdout:
            print(line.strip())
        process.wait()

        if process.returncode == 0:
            logging.info(f"Rsync completed: {source_folder} -> {destination_folder}")

            if days_threshold:
                logging.info(f"Removing files older than {days_threshold} days from {source_folder}")
                find_command = ["find", source_folder, "-type", "f", "-mtime", f"+{days_threshold}", "-delete"]
                subprocess.run(find_command, check=True)
        else:
            logging.error(f"Rsync failed with code {process.returncode}")
            raise subprocess.CalledProcessError(process.returncode, command)
    except Exception as e:
        logging.error(f"Rsync failed: {e}")
        raise


def set_global_umask(umask_value):
    """
    Set global umask for directory and file permissions.
    """
    try:
        os.umask(int(umask_value, 8))
        logging.info(f"Set umask to {umask_value}")
    except ValueError as e:
        logging.error(f"Invalid umask value: {umask_value} - {e}")
        sys.exit(1)


def parse_arguments():
    """
    Parse command-line arguments.
    """
    parser = argparse.ArgumentParser(description="Process video backups using rsync or move.")
    parser.add_argument("--action", choices=["rsync", "move"], help="Specify action (rsync or move). Overrides config.")
    return parser.parse_args()


def get_config_or_exit(config, key, description):
    """
    Retrieve a configuration value or exit with an error.
    """
    value = config.get(key)
    if value is None:
        logging.error(f"Missing configuration: {description}")
        sys.exit(f"Missing configuration: {description}")
    return value


def main():
    # Parse arguments and load configuration
    args = parse_arguments()
    config = load_config(CONFIG_FILE)

    # Determine action
    action = args.action or get_config_or_exit(config, "action", "action (rsync or move)")

    # Initialize logging
    log_file = initialize_logging(
        get_config_or_exit(config, "log_path", "log path"),
        get_config_or_exit(config, "log_file_format", "log file format"),
        get_config_or_exit(config, "log_format", "log format")
    )

    # Set umask
    set_global_umask(get_config_or_exit(config, "umask", "umask value"))

    # Load paths and settings
    remote_path = get_config_or_exit(config, "remote_path", "remote path")
    local_path = get_config_or_exit(config, "local_path", "local path")
    rsync_options = get_config_or_exit(config, "rsync_options", "rsync options")
    history_file = get_config_or_exit(config, "history_file", "history file")
    days_threshold = int(config.get("days_threshold", 0)) if action == "rsync" else None

    processed_folders = load_history(history_file)

    logging.info(f"Starting script with action: {action}")
    folders = [f for f in os.listdir(remote_path) if os.path.isdir(os.path.join(remote_path, f))]
    logging.info(f"Total folders found: {len(folders)}")

    start_time = time()

    for i, folder in enumerate(folders, 1):
        if folder in processed_folders:
            logging.info(f"Skipping already processed folder: {folder}")
            continue

        if not validate_folder_name(folder):
            logging.warning(f"Invalid folder name skipped: {folder}")
            continue

        source_folder = os.path.join(remote_path, folder)
        destination_folder = ensure_path_structure(local_path, folder)

        logging.info(f"Processing folder {i}/{len(folders)}: {folder}")
        try:
            if action == "rsync":
                rsync_folder(source_folder, destination_folder, rsync_options, days_threshold)
            elif action == "move":
                move_folder(source_folder, destination_folder)

            update_history(history_file, folder)
        except Exception as e:
            logging.error(f"Error processing folder: {e}")
            continue

    elapsed_time = time() - start_time
    logging.info(f"All operations completed in {elapsed_time:.2f} seconds.")


if __name__ == "__main__":
    main()
