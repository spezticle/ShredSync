#!/usr/bin/env python3
import os
import shutil
import argparse
import logging
import filecmp

def initialize_logging(log_file):
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    logging.getLogger().addHandler(logging.StreamHandler())  # Print to console as well

def is_clone(folder, subfolder):
    """
    Check if a subfolder is a clone of its parent folder by comparing contents.
    """
    try:
        comparison = filecmp.dircmp(folder, subfolder)
        # Check if files and subdirectories match exactly
        return not comparison.left_only and not comparison.right_only and not comparison.diff_files
    except Exception as e:
        logging.error(f"Error comparing {folder} and {subfolder}: {e}")
        return False

def find_cloned_nested_folders(source):
    """
    Recursively find folders where a folder contains another folder with the same name and identical contents.
    """
    cloned_folders = []
    for root, dirs, files in os.walk(source):
        for dir_name in dirs:
            parent_folder = os.path.join(root, dir_name)
            nested_folder = os.path.join(parent_folder, dir_name)
            if os.path.isdir(nested_folder) and is_clone(parent_folder, nested_folder):
                cloned_folders.append(nested_folder)
    return cloned_folders

def move_folder(folder, destination):
    """
    Move the folder to the destination.
    """
    try:
        base_name = os.path.basename(folder)
        dest_path = os.path.join(destination, base_name)
        shutil.move(folder, dest_path)
        logging.info(f"Moved: {folder} -> {dest_path}")
    except Exception as e:
        logging.error(f"Failed to move {folder}: {e}")

def main():
    parser = argparse.ArgumentParser(description="Find and move cloned nested folders.")
    parser.add_argument("--source", required=True, help="Source directory to search")
    parser.add_argument("--destination", required=True, help="Destination directory for moving folders")
    parser.add_argument("--log", default="nested_folder_fix.log", help="Log file name (default: nested_folder_fix.log)")
    args = parser.parse_args()

    source = args.source
    destination = args.destination
    log_file = args.log

    # Initialize logging
    initialize_logging(log_file)
    logging.info("Starting cloned nested folder finder script.")
    logging.info(f"Source: {source}")
    logging.info(f"Destination: {destination}")

    # Validate paths
    if not os.path.isdir(source):
        logging.error(f"Source path does not exist or is not a directory: {source}")
        return
    if not os.path.exists(destination):
        try:
            os.makedirs(destination)
            logging.info(f"Created destination directory: {destination}")
        except Exception as e:
            logging.error(f"Failed to create destination directory: {e}")
            return

    # Find and move cloned nested folders
    cloned_folders = find_cloned_nested_folders(source)
    if not cloned_folders:
        logging.info("No cloned nested folders found.")
        return

    logging.info(f"Found {len(cloned_folders)} cloned nested folders.")
    for folder in cloned_folders:
        move_folder(folder, destination)

    logging.info("Script completed.")

if __name__ == "__main__":
    main()
