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
sys.stdout.reconfigure(line_buffering=True)
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


def get_folder_age(folder_path):
    """
    Calculate the age of a folder in days based on its last modification time.
    """
    folder_mtime = os.path.getmtime(folder_path)
    age_in_days = (time() - folder_mtime) / (24 * 3600)
    return age_in_days


def delete_old_folder(folder_path, days_threshold, dry_run=False):
    """
    Delete a folder if it exceeds the age threshold.
    """
    age_in_days = get_folder_age(folder_path)
    if age_in_days > days_threshold:
        if dry_run:
            logging.info(f"DRY-RUN: Would delete folder {folder_path}, age: {age_in_days:.2f} days")
        else:
            try:
                shutil.rmtree(folder_path)
                logging.info(f"Deleted folder {folder_path}, age: {age_in_days:.2f} days")
            except Exception as e:
                logging.error(f"Failed to delete folder {folder_path}: {e}")
    else:
        logging.info(f"Folder {folder_path} is {age_in_days:.2f} days old, below threshold {days_threshold} days")


def detect_and_fix_nesting(destination_path, dry_run):
    """
    Detect and fix nested folders in the destination path.
    """
    logging.info(f"Checking for nested folders in {destination_path}...")
    changes = []

    for root, dirs, _ in os.walk(destination_path, topdown=False):
        for dir_name in dirs:
            dir_path = os.path.join(root, dir_name)
            sub_dirs = [d for d in os.listdir(dir_path) if os.path.isdir(os.path.join(dir_path, d))]

            # Check for nesting
            if len(sub_dirs) == 1 and sub_dirs[0] == dir_name:
                nested_path = os.path.join(dir_path, sub_dirs[0])
                logging.warning(f"Nested folder detected: {nested_path}")

                # Suggested fix: move contents of the nested folder to the parent folder
                changes.append((nested_path, dir_path))

                if dry_run:
                    logging.info(f"DRY-RUN: Would fix nesting by moving contents of {nested_path} to {dir_path}")
                else:
                    # Move nested folder contents to the parent folder
                    for item in os.listdir(nested_path):
                        item_path = os.path.join(nested_path, item)
                        target_path = os.path.join(dir_path, item)
                        shutil.move(item_path, target_path)
                        logging.info(f"Moved: {item_path} -> {target_path}")

                    # Delete the now-empty nested folder
                    if not os.listdir(nested_path):  # Check if the folder is empty
                        os.rmdir(nested_path)
                        logging.info(f"Deleted empty folder: {nested_path}")
                    else:
                        logging.warning(f"Nested folder not empty, not deleted: {nested_path}")

    if dry_run and changes:
        logging.info("Detected nested folders that would be fixed:")
        for nested_path, parent_path in changes:
            logging.info(f"Nested: {nested_path} -> Parent: {parent_path}")
    elif not changes:
        logging.info("No nested folders detected.")


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


def ensure_path_structure(local_path, folder, dry_run=False):
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
        full_name[len("import-"):]
    )
    if not dry_run:
        os.makedirs(destination, exist_ok=True)
    logging.info(f"Destination structure ensured: {destination}")
    return destination


def move_folder(source_folder, destination_folder, dry_run=False):
    """
    Move the folder to the destination.
    """
    try:
        if dry_run:
            logging.info(f"DRY-RUN: Would move folder: {source_folder} -> {destination_folder}")
        else:
            shutil.move(source_folder, destination_folder)
            logging.info(f"Moved folder: {source_folder} -> {destination_folder}")
    except Exception as e:
        logging.error(f"Failed to move folder: {e}")
        raise


def rsync_folder(source_folder, destination_folder, rsync_options, days_threshold=None, dry_run=False):
    """
    Rsync the folder and optionally remove old source files based on age.
    """
    try:
        if dry_run:
            logging.info(f"DRY-RUN: Would rsync folder: {source_folder} -> {destination_folder}")
            return

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

                logging.info(f"Removing empty directories from {source_folder}")
                find_empty_dirs_command = ["find", source_folder, "-type", "d", "-empty", "-delete"]
                subprocess.run(find_empty_dirs_command, check=True)
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
    parser.add_argument("--dry-run", action="store_true", help="Perform a dry run without making changes.")
    parser.add_argument("--fix-nesting", action="store_true", help="Check and fix nested folders in destination path.")
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
    dry_run = args.dry_run
    fix_nesting = args.fix_nesting

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

    # Check and fix nesting if requested
    if fix_nesting:
        logging.info("Running fix-nesting operation.")
        detect_and_fix_nesting(local_path, dry_run)
        logging.info("Fix-nesting operation complete. Exiting.")
        sys.exit(0)

    # Proceed with the sync or move operation
    logging.info(f"Starting script with action: {action}")
    folders = [f for f in os.listdir(remote_path) if os.path.isdir(os.path.join(remote_path, f))]
    logging.info(f"Total folders found: {len(folders)}")

    start_time = time()

    for i, folder in enumerate(folders, 1):
        folder_path = os.path.join(remote_path, folder)

        # Skip transfer/copy for already processed folders, but check for deletion
        if folder in processed_folders:
            logging.info(f"Skipping transfer/copy for already processed folder: {folder}")
            delete_old_folder(folder_path, days_threshold, dry_run)
            continue

        # Validate folder name
        if not validate_folder_name(folder):
            logging.warning(f"Invalid folder name skipped: {folder}")
            continue

        destination_folder = ensure_path_structure(local_path, folder, dry_run)

        logging.info(f"Processing folder {i}/{len(folders)}: {folder}")
        try:
            # Perform transfer/copy operation
            if action == "rsync":
                rsync_folder(folder_path, destination_folder, rsync_options, days_threshold, dry_run)
            elif action == "move":
                move_folder(folder_path, destination_folder, dry_run)

            # Update history only if the folder was successfully processed
            if not dry_run:
                update_history(history_file, folder)

            # Check for deletion after processing
            delete_old_folder(folder_path, days_threshold, dry_run)

        except Exception as e:
            logging.error(f"Error processing folder {folder}: {e}")
            continue

    elapsed_time = time() - start_time
    logging.info(f"All operations completed in {elapsed_time:.2f} seconds.")


if __name__ == "__main__":
    main()
