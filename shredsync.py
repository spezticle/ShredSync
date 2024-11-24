#!/opt/ShredBackupSync/venv/bin/python3
import os
import sys
import logging
import subprocess
import yaml
import re
import stat
from datetime import datetime

CONFIG_FILE = "/opt/ShredBackupSync/config.yaml"
HISTORY_FILE = "/opt/ShredBackupSync/processed_folders.log"

def load_config(config_file):
    with open(config_file, "r") as file:
        return yaml.safe_load(file)

def initialize_logging(log_path, log_dir_permission, log_file_permission):
#   Initialize logging with directory and file permission checks.
#   Ensure log_path exists with proper permissions, 
#   create log file if necessary, and ensure it has correct permissions.

    # Convert permissions from string to integer
    log_dir_permission = int(log_dir_permission, 8)
    log_file_permission = int(log_file_permission, 8)
    # Ensure the log directory exists
    if not os.path.exists(log_path):
        try:
            os.makedirs(log_path, mode=log_dir_permission)
        except Exception as e:
            sys.exit(1)
    else:
        # Check and fix permissions for the log directory
        current_dir_permissions = stat.S_IMODE(os.stat(log_path).st_mode)
        if current_dir_permissions != log_dir_permission:
            try:
                os.chmod(log_path, log_dir_permission)
            except Exception as e:
                sys.exit(1)

    log_file = os.path.join(log_path, f"{datetime.now().strftime('%Y%m%d')}.log")
    if not os.path.exists(log_file):
        try:
            # Create an empty file with proper permissions
            with open(log_file, 'w') as file:
                pass
            os.chmod(log_file, log_file_permission)
        except Exception as e:
            sys.exit(1)
    else:
        # Check and fix permissions for the log file
        current_file_permissions = stat.S_IMODE(os.stat(log_file).st_mode)
        if current_file_permissions != log_file_permission:
            try:
                os.chmod(log_file, log_file_permission)
            except Exception as e:
                sys.exit(1)

    logging.basicConfig(
        filename=log_file,
        level=logging.DEBUG,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    return log_file

def load_history(history_file):
    if not os.path.exists(history_file):
        return set()
    with open(history_file, "r") as file:
        return set(line.strip() for line in file)

def update_history(history_file, folder):
    """
    Append a successfully processed folder to the history file.
    """
    try:
        with open(history_file, "a") as file:
            file.write(f"{folder}\n")
        logging.info(f"Updated history log with folder: {folder}")
    except Exception as e:
        logging.error(f"Failed to update history log for folder {folder}: {e}")
        raise


def validate_folder_name(folder_name):
    """
    Validate folder name against the expected format: import-name-date-uuid.
    """
    pattern = r"^import-[a-zA-Z0-9_ -]+-\d{12}-[a-fA-F0-9-]{36}$"
    logging.debug(f"Folder Patern: {pattern}")
    return re.match(pattern, folder_name) is not None

def get_remote_folders(remote_path, ssh_user, ssh_host, ssh_port, identity_file):
    """
    Retrieve the list of all remote folders.
    """
    logging.info("Retrieving remote folder list...")
    command = (
        f"ssh -i {identity_file} {ssh_user}@{ssh_host} -p {ssh_port} "
        f"'stat -f \"%N|%m\" {remote_path}/*'"
    )

    try:
        result = subprocess.run(command, shell=True, capture_output=True, text=True, check=True)
    except subprocess.CalledProcessError as e:
        logging.error(f"Error executing SSH command: {e.stderr.strip()}")
        raise RuntimeError("Failed to retrieve remote folders.") from e

    raw_output = result.stdout.strip()
    folders = []

    for line in raw_output.splitlines():
        line = line.strip()
        if not line or '|' not in line:
            logging.warning(f"Unexpected line format: {line}")
            continue

        try:
            full_path, mod_time = line.split('|', 1)
            folder_name = os.path.basename(full_path)  # Extract folder name from path
            mod_time = float(mod_time)
            folders.append((folder_name, mod_time))
        except ValueError as e:
            logging.warning(f"Error parsing line: {line}. Exception: {str(e)}")
            continue
    logging.info(f"{len(folders)} folders found")
    return folders

def ensure_path_structure(local_path, folder):
    """
    Ensure the destination path structure exists based on the folder name.
    """
    pattern = r"^(import-[a-zA-Z0-9_ -]+)-(\d{12})-([a-fA-F0-9-]{36})$"
    match = re.match(pattern, folder)
    if not match:
        raise ValueError(f"Folder name does not match the expected pattern: {folder}")

    full_name, timestamp, uuid = match.groups()
    try:
        date = datetime.strptime(timestamp, "%Y%m%d%H%M")
    except ValueError:
        raise ValueError(f"Timestamp in folder name is invalid: {timestamp}")

    year = date.strftime("%Y")
    month = date.strftime("%m-%B")
    day = date.strftime("%d-%A")

    name = full_name[len("import-"):]  # Strip "import-" prefix from the name

    destination = os.path.join(local_path, year, month, day, name, folder)
    logging.debug(f"Creating directory structure: {destination}")
    try:
        os.makedirs(destination, exist_ok=True)
        logging.info(f"Directory created or already exists: {destination}")
    except Exception as e:
        logging.error(f"Failed to create directory {destination}: {str(e)}")
        raise RuntimeError(f"Could not create directory: {destination}") from e
    return destination

def rsync_folder(remote_path, folder, local_path, ssh_user, ssh_host, ssh_port, identity_file):
    """
    Rsync the specified folder from the remote path to the local path.
    Display status updates and verify if the folder already exists locally.
    """
    remote_folder = f"{remote_path}/{folder}"
    destination_folder = ensure_path_structure(local_path, folder)

    command = [
        "rsync", "-avz", "-e", f"ssh -p {ssh_port} -i {identity_file}",
        f"{ssh_user}@{ssh_host}:{remote_folder}", destination_folder
    ]
    print(f"Starting rsync for folder: {folder}")
    logging.info(f"Starting rsync: {remote_folder} -> {destination_folder}")

    try:
        result = subprocess.run(command, text=True)
        if result.returncode == 0:
            print(f"Rsync completed successfully for folder: {folder}")
            logging.info(f"Rsync completed for folder: {folder}")
        else:
            raise RuntimeError(f"Rsync failed with return code {result.returncode}")
    except Exception as e:
        logging.error(f"Error syncing folder {folder}: {str(e)}")
        print(f"Error syncing folder {folder}: {str(e)}")
        raise

def delete_remote_folder(remote_path, folder, mod_time, ssh_user, ssh_host, ssh_port, identity_file, days_threshold):
    """
    Deletes a folder from the remote server if it is older than the specified threshold.
    """
    now = datetime.now().timestamp()
    if (now - mod_time) <= (days_threshold * 86400):  # Not older than threshold
        logging.info(f"Skipping deletion for folder (not old enough): {folder}")
        print(f"Skipping delete for folder (not old enough): {folder}")
        return

    remote_folder = f"{remote_path}/{folder}"
    command = ["ssh", "-i", identity_file, f"{ssh_user}@{ssh_host} -p {ssh_port} ", f"rm -rf {remote_folder}"]
    logging.info(f"Deleting remote folder: {remote_folder}")
    try:
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        if result.returncode == 0:
            logging.info(f"Successfully deleted remote folder: {remote_folder}")
        else:
            raise RuntimeError(f"Deletion failed with return code {result.returncode}")
    except Exception as e:
        logging.error(f"Error deleting remote folder {remote_folder}: {str(e)}")
        print(f"Error deleting remote folder {remote_folder}: {str(e)}")

def set_global_umask(umask_value):
    """
    Set the global umask for the script.
    """
    try:
        umask = int(umask_value, 8)  # Convert the octal string to an integer
        os.umask(umask)
        logging.info(f"Set global umask to {oct(umask)}")
        print(f"Global umask set to {oct(umask)}")
    except ValueError as e:
        logging.error(f"Invalid umask value provided in config: {umask_value}. Error: {e}")
        print(f"Error: Invalid umask value '{umask_value}' in config.")
        sys.exit(1)

# Begin main
def main():
    config = load_config(CONFIG_FILE)
    log_path = config["log_path"]
    log_dir_permission = config.get("log_dir_permission", 0o755)
    log_file_permission = config.get("log_file_permission", 0o644)
    log_file = initialize_logging(log_path, log_dir_permission, log_file_permission)
    umask_value = config.get("umask", "022")
    set_global_umask(umask_value)

    remote_path = config["remote_path"]
    local_path = config["local_path"]
    ssh_user = config["ssh_user"]
    ssh_host = config["ssh_host"]
    ssh_port = config["ssh_port"]
    identity_file = config["identity_file"]
    days_threshold = config["days_threshold"]

    logging.info("Starting ShredBackupSync script.")
    if len(sys.argv) < 2:
        logging.error("No action provided. Use 'list', 'sync delete', or 'sync nodelete'.")
        sys.exit("Usage: script.py list|sync delete|sync nodelete [verify]")

    action = sys.argv[1].lower()
    verify = "verify" in sys.argv
    delete = "delete" in sys.argv

    processed_folders = load_history(HISTORY_FILE)

    try:
        folders = get_remote_folders(remote_path, ssh_user, ssh_host, ssh_port, identity_file)
        logging.info(f"{len(folders)} folders found on remote.")

        if action == "list":
            print("Folders to process:")
            for folder, _ in folders:
                print(folder)
            return

        for folder, mod_time in folders:
            if folder in processed_folders and not verify:
                logging.info(f"Skipping already processed folder: {folder}")
                continue

            try:
                # Process the folder
                rsync_folder(remote_path, folder, local_path, ssh_user, ssh_host, ssh_port, identity_file)
                logging.info(f"Successfully synced folder: {folder}")

                # If delete is specified, delete the folder
                if delete:
                    delete_remote_folder(remote_path, folder, mod_time, ssh_user, ssh_host, ssh_port, identity_file, days_threshold)

                # Update history log only after successful processing
                update_history(HISTORY_FILE, folder)
            except Exception as e:
                logging.error(f"Error processing folder {folder}: {e}")
                continue

        logging.info("All operations completed.")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        logging.shutdown()
        print(f"Log written to {log_file}")

if __name__ == "__main__":
    main()