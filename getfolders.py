#!/opt/ShredBackupSync/venv/bin/python3
import subprocess
remote_path = "ais-shred-1:/Volumes/shred/shred_backup"
ssh_user = "shred"
identity_file = "/tank/shred/.ssh/id_ed25519"
command = f"ssh -i {identity_file} {ssh_user}@{remote_path.split(':')[0]} 'gfind {remote_path.split(':')[1]} -mindepth 1 -maxdepth 1 -type d -printf \"%P|%T@\\n\"'"
output = subprocess.run(command, shell=True, capture_output=True, text=True, check=True).stdout.strip()
print(output)