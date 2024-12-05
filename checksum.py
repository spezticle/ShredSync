#!/usr/bin/python3
import os
import sys
import logging
import subprocess
import yaml
from time import time

CONFIG_FILE = "checksum.yaml"

def load_config(config_file):
    with open(config_file, "r") as file:
        return yaml.safe_load(file)

def initialize_logging(log_path, log_file, log_format):
    """
    Initialize logging.
    """
    os.makedirs(log_path, exist_ok=True)
    log_file = os.path.join(log_path, log_file)
    logging.basicConfig(
        filename=log_file,
        level=logging.DEBUG,
        format=log_format
    )
    return log_file

def validate_paths(remote_path, destination_path):
    """
    Validate the existence of source (remote_path) and destination (destination_path).
    """
    if not os.path.exists(remote_path):
        raise ValueError(f"Remote path does not exist: {remote_path}")
    if not os.path.exists(destination_path):
        os.makedirs(destination_path)
        logging.info(f"Created destination path: {destination_path}")

def rsync_move(source_folder, destination_folder):
    """
    Use rsync to move files with checksum verification and remove source files.
    """
    command = [
        "rsync", "-avz", "--info=progress2", "--checksum", "--remove-source-files", f"{source_folder}/", f"{destination_folder}/"
    ]
    logging.info(f"Running rsync command: {' '.join(command)}")

    try:
        result = subprocess.run(command, check=True, capture_output=True, text=True)
        logging.info(result.stdout)
        logging.info(f"Rsync completed: {source_folder} -> {destination_folder}")
        # Remove empty directories after files have been moved
        remove_empty_dirs(source_folder)
    except subprocess.CalledProcessError as e:
        logging.error(f"Rsync failed: {e.stderr}")
        raise

def remove_empty_dirs(directory):
    """
    Recursively remove empty directories.
    """
    for root, dirs, files in os.walk(directory, topdown=False):
        for dir_name in dirs:
            dir_path = os.path.join(root, dir_name)
            try:
                os.rmdir(dir_path)
                logging.info(f"Removed empty directory: {dir_path}")
            except OSError:
                # Directory not empty or permission issue; skip
                pass
    try:
        os.rmdir(directory)
        logging.info(f"Removed top-level empty directory: {directory}")
    except OSError:
        # Top-level directory not empty
        pass

def process_folders(remote_path, destination_path, history_file):
    """
    Process folders in the remote path and move them to the destination path.
    """
    processed_folders = load_history(history_file)
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

        source_folder = os.path.join(remote_path, folder)
        destination_folder = os.path.join(destination_path, folder)

        logging.info(f"Processing folder {i}/{total_folders}: {folder}")
        print(f"Processing folder {i}/{total_folders}: {folder}")

        try:
            rsync_move(source_folder, destination_folder)
            update_history(history_file, folder)
        except Exception as e:
            logging.error(f"Error processing folder {folder}: {e}")
            continue

    elapsed_time = time() - start_time
    logging.info(f"All operations completed in {elapsed_time:.2f} seconds.")
    print(f"All operations completed in {elapsed_time:.2f} seconds.")

def load_history(history_file):
    """
    Load processed folders from the history file.
    """
    if not os.path.exists(history_file):
        return set()
    with open(history_file, "r") as file:
        return set(line.strip() for line in file)

def update_history(history_file, folder):
    """
    Update the history file with the processed folder.
    """
    try:
        with open(history_file, "a") as file:
            file.write(f"{folder}\n")
        logging.info(f"Updated history with folder: {folder}")
    except Exception as e:
        logging.error(f"Failed to update history for folder {folder}: {e}")

def main():
    config = load_config(CONFIG_FILE)

    # Initialize logging
    log_path = config["log_path"]
    log_file = config.get("log_file_format", "rsync_move.log")
    log_format = config.get("log_format", "%(asctime)s - %(levelname)s - %(message)s")
    initialize_logging(log_path, log_file, log_format)

    # Validate paths
    remote_path = config["remote_path"]
    destination_path = config["destination_path"]
    history_file = config["history_file"]

    logging.info("Starting folder processing...")
    try:
        validate_paths(remote_path, destination_path)
        process_folders(remote_path, destination_path, history_file)
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        print(f"Error: {e}")
    finally:
        logging.shutdown()

if __name__ == "__main__":
    main()
