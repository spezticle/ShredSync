#!/opt/ShredBackupSync/venv/bin/python3

import subprocess
import yaml
import sys

# Load configuration from YAML file
CONFIG_FILE = "/opt/ShredBackupSync/config.yaml"

def load_config(config_file):
    with open(config_file, "r") as file:
        return yaml.safe_load(file)

def get_remote_folders(remote_path, ssh_user, identity_file):
    """
    Fetch the list of folders from the remote source using SSH and `gfind`.
    """
    remote_host, remote_dir = remote_path.split(':')
    command = [
        "ssh", "-i", identity_file, f"{ssh_user}@{remote_host}",
        f"gfind {remote_dir} -mindepth 1 -maxdepth 1 -type d -printf '%P|%T@\n'"
    ]
    
    try:
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        raw_output = result.stdout.strip()
        print("Raw output from remote command:")
        print(raw_output)
        print("\nParsed folders:")
        
        for line in raw_output.splitlines():
            if not line.strip():
                continue
            if '|' not in line:
                print(f"Unexpected format: {line}")
                continue
            name, mod_time = line.split('|', 1)
            print(f"Folder: {name}, Last Modified: {mod_time}")
    except subprocess.CalledProcessError as e:
        print(f"Error retrieving remote folder list: {e.stderr.strip()}")
        sys.exit(1)

if __name__ == "__main__":
    config = load_config(CONFIG_FILE)
    remote_path = config["remote_path"]
    ssh_user = config["ssh_user"]
    identity_file = config["identity_file"]

    print("Fetching remote folders...")
    get_remote_folders(remote_path, ssh_user, identity_file)