#!/usr/bin/env python3
import os
import shutil
import filecmp
import argparse
import logging

def initialize_logging(log_file):
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    logging.getLogger().addHandler(logging.StreamHandler())  # Print to console as well

def build_master_list(master_path):
    """
    Build a list of subfolders in the master directory.
    """
    return [
        folder for folder in os.listdir(master_path)
        if os.path.isdir(os.path.join(master_path, folder))
    ]

def compare_and_clean(master_path, target_path):
    """
    Compare subfolders in the target path with the master path.
    If a subfolder in the target matches one in the master and all files exist, delete the target subfolder.
    """
    master_list = build_master_list(master_path)
    target_list = build_master_list(target_path)

    logging.info(f"Master path: {master_path}")
    logging.info(f"Target path: {target_path}")
    logging.info(f"Master list: {master_list}")
    logging.info(f"Target list: {target_list}")

    for subfolder in target_list:
        if subfolder in master_list:
            master_subfolder = os.path.join(master_path, subfolder)
            target_subfolder = os.path.join(target_path, subfolder)

            # Compare the contents
            logging.info(f"Comparing {target_subfolder} with {master_subfolder}")
            if compare_folders(master_subfolder, target_subfolder):
                logging.info(f"Deleting matched folder: {target_subfolder}")
                delete_folder(target_subfolder)
            else:
                logging.info(f"Folders differ: {target_subfolder} not deleted.")

def compare_folders(folder1, folder2):
    """
    Compare two folders to ensure all files in folder2 exist in folder1.
    """
    comparison = filecmp.dircmp(folder1, folder2)

    # Ensure no files are unique to folder2
    if comparison.right_only or comparison.diff_files:
        logging.debug(f"Unique to {folder2}: {comparison.right_only}")
        logging.debug(f"Different files: {comparison.diff_files}")
        return False

    # Recursively check subdirectories
    for subdir in comparison.common_dirs:
        subfolder1 = os.path.join(folder1, subdir)
        subfolder2 = os.path.join(folder2, subdir)
        if not compare_folders(subfolder1, subfolder2):
            return False

    return True

def delete_folder(folder):
    """
    Delete a folder recursively.
    """
    try:
        shutil.rmtree(folder)
        logging.info(f"Deleted folder: {folder}")
    except Exception as e:
        logging.error(f"Failed to delete folder {folder}: {e}")

def main():
    parser = argparse.ArgumentParser(description="Compare and clean folders.")
    parser.add_argument("--master", required=True, help="Path to the master folder (e.g., unfuck1)")
    parser.add_argument("--target", required=True, help="Path to the target folder (e.g., unfuck2)")
    parser.add_argument("--log", default="compare_clean.log", help="Log file name (default: compare_clean.log)")
    args = parser.parse_args()

    master_path = args.master
    target_path = args.target
    log_file = args.log

    # Initialize logging
    initialize_logging(log_file)
    logging.info("Starting folder comparison script.")
    logging.info(f"Master: {master_path}")
    logging.info(f"Target: {target_path}")

    # Validate paths
    if not os.path.isdir(master_path):
        logging.error(f"Master path does not exist or is not a directory: {master_path}")
        return
    if not os.path.isdir(target_path):
        logging.error(f"Target path does not exist or is not a directory: {target_path}")
        return

    # Compare and clean
    compare_and_clean(master_path, target_path)

    logging.info("Script completed.")

if __name__ == "__main__":
    main()
