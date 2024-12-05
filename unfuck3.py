#!/usr/bin/env python3
import os
import shutil
import argparse
import logging

def initialize_logging(log_file):
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    logging.getLogger().addHandler(logging.StreamHandler())  # Print to console as well

def find_deepest_nested_folders(source):
    """
    Recursively find folders where a folder contains another folder with the same name.
    Collect all occurrences in a hierarchy for reporting.
    """
    nested_folder_data = {}
    for root, dirs, _ in os.walk(source):
        for dir_name in dirs:
            nested_path = os.path.join(root, dir_name)
            if dir_name not in nested_folder_data:
                nested_folder_data[dir_name] = []
            if nested_path.endswith(f"/{dir_name}"):
                nested_folder_data[dir_name].append(nested_path)
    return {key: paths for key, paths in nested_folder_data.items() if len(paths) > 1}

def find_deepest_path(matches):
    """
    Find the deepest path from a list of folder paths.
    """
    return max(matches, key=lambda path: path.count(os.sep))

def move_deepest_folder(folder, destination):
    """
    Move the deepest folder to the destination.
    """
    try:
        base_name = os.path.basename(folder)
        dest_path = os.path.join(destination, base_name)
        shutil.move(folder, dest_path)
        logging.info(f"Moved deepest folder: {folder} -> {dest_path}")
        print(f"Moved deepest folder: {folder} -> {dest_path}")
    except Exception as e:
        logging.error(f"Failed to move {folder}: {e}")

def report_and_move_nested_folders(nested_folders, destination):
    """
    Report all deeply nested folders and their matches, and move the deepest one.
    """
    for name, matches in nested_folders.items():
        print(f"Deep nest found.")
        print(f"Name: {name}")
        print("Matches:")
        for path in matches:
            print(f"  - {path}")

        # Find and move the deepest folder
        deepest_folder = find_deepest_path(matches)
        print(f"\nMoving deepest folder: {deepest_folder} -> {destination}")
        move_deepest_folder(deepest_folder, destination)

def main():
    parser = argparse.ArgumentParser(description="Find and move deeply nested folders with repeated names.")
    parser.add_argument("--source", required=True, help="Source directory to search")
    parser.add_argument("--destination", required=True, help="Destination directory for moving folders")
    parser.add_argument("--log", default="nested_folder_fix.log", help="Log file name (default: nested_folder_fix.log)")
    args = parser.parse_args()

    source = args.source
    destination = args.destination
    log_file = args.log

    # Initialize logging
    initialize_logging(log_file)
    logging.info("Starting nested folder finder script.")
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

    # Find deeply nested folders
    nested_folders = find_deepest_nested_folders(source)
    if not nested_folders:
        logging.info("No deeply nested folders found.")
        return

    logging.info(f"Found {len(nested_folders)} deeply nested folders.")
    for name, matches in nested_folders.items():
        logging.info(f"Name: {name}")
        logging.info("Matches:")
        for path in matches:
            logging.info(f"  - {path}")

    # Report and move deepest folders
    report_and_move_nested_folders(nested_folders, destination)

    logging.info("Script completed.")

if __name__ == "__main__":
    main()
