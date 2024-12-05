#!/usr/bin/env python3
import os
import hashlib
import argparse
import logging
from pathlib import Path


def initialize_logging(log_file):
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    logging.getLogger().addHandler(logging.StreamHandler())  # Print to console as well


def calculate_folder_size(folder):
    """
    Calculate the total size of files in a folder.
    """
    total_size = 0
    for root, _, files in os.walk(folder):
        for file in files:
            file_path = os.path.join(root, file)
            total_size += os.path.getsize(file_path)
    return total_size


def generate_md5(file_path):
    """
    Generate an MD5 hash for the given file.
    """
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def compare_and_clean(master_folder, target_folder, archive_path):
    """
    Compare files in matching folders between master and target.
    Delete files in target if they exist in master and are identical.
    """
    master_files = {file.name: file for file in Path(master_folder).iterdir() if file.is_file()}
    target_files = {file.name: file for file in Path(target_folder).iterdir() if file.is_file()}

    logging.info(f"Comparing files in {target_folder} against {master_folder}")

    for file_name, target_file in target_files.items():
        if file_name in master_files:
            master_file = master_files[file_name]
            if generate_md5(master_file) == generate_md5(target_file):
                logging.info(f"File {file_name} is identical. Deleting from target: {target_file}")
                try:
                    os.remove(target_file)
                except Exception as e:
                    logging.error(f"Failed to delete {target_file}: {e}")
            else:
                logging.info(f"File {file_name} differs between master and target.")
        else:
            logging.info(f"File {file_name} exists only in target: {target_file}")


def process_folders(master, target, archive):
    """
    Process and compare folders between master and target paths.
    """
    master_folders = [folder for folder in Path(master).iterdir() if folder.is_dir()]
    target_folders = [folder for folder in Path(target).iterdir() if folder.is_dir()]

    logging.info(f"Master folders: {master_folders}")
    logging.info(f"Target folders: {target_folders}")

    for master_folder in master_folders:
        matching_folder = next((tf for tf in target_folders if tf.name == master_folder.name), None)
        if matching_folder:
            logging.info(f"Found matching folder: {master_folder.name}")
            master_size = calculate_folder_size(master_folder)
            target_size = calculate_folder_size(matching_folder)

            logging.info(f"Master folder size: {master_size} bytes")
            logging.info(f"Target folder size: {target_size} bytes")

            if master_size > target_size:
                logging.info(f"Master folder {master_folder.name} is larger. Checking contents...")
                compare_and_clean(master_folder, matching_folder, archive)
            else:
                logging.info(f"Target folder {matching_folder.name} is equal or larger. No action taken.")
        else:
            logging.info(f"No matching folder for {master_folder.name} in target.")


def main():
    parser = argparse.ArgumentParser(description="Compare and sync folders.")
    parser.add_argument("--master", required=True, help="Path to the master directory.")
    parser.add_argument("--target", required=True, help="Path to the target directory.")
    parser.add_argument("--archive", required=True, help="Path to the archive directory (not used yet).")
    parser.add_argument("--log", default="compare_sync.log", help="Log file name (default: compare_sync.log)")
    args = parser.parse_args()

    master = args.master
    target = args.target
    archive = args.archive
    log_file = args.log

    # Initialize logging
    initialize_logging(log_file)
    logging.info("Starting folder comparison script.")
    logging.info(f"Master: {master}")
    logging.info(f"Target: {target}")
    logging.info(f"Archive: {archive}")

    # Validate paths
    for path in [master, target, archive]:
        if not os.path.isdir(path):
            logging.error(f"Path does not exist or is not a directory: {path}")
            return

    # Process folders
    process_folders(master, target, archive)

    logging.info("Script completed.")


if __name__ == "__main__":
    main()
