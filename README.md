# ShredBackupSync Script

## Overview
`ShredBackupSync` is a Python script designed for efficient backup synchronization of remote folders with advanced features like verification and deletion options. This document provides details on the required dependencies, configuration, and usage instructions.

---

## Files and Their Purpose

### `requirements.txt`
This file contains the list of Python dependencies required to run the script. It is used to set up the Python environment consistently across different machines. To install the dependencies, use:

```bash
pip install -r requirements.txt
```
config.yaml

This configuration file is used to define customizable settings for the script, such as:

	•	Logging configurations: Directory, file format, and permissions.
	•	Remote server details: SSH credentials and host information.
	•	Threshold settings: Parameters like days_threshold for filtering folders.
	•	History file location: Path to track processed folders.


 Usage

The script can be executed with the following commands:

Syntax
```script.py list|sync delete|sync nodelete [verify]```

Commands

	•	list: Lists folders on the remote server that are available for processing.
	•	sync delete: Synchronizes folders to the local machine and deletes them from the remote server after processing.
	•	sync nodelete: Synchronizes folders to the local machine without deleting them from the remote server.
	•	[verify] (Optional): Forces reprocessing of folders, even if they are marked as processed.

Python Requirements

Python Version

	•	Python 3.8 or higher is required to run this script.

Modules

The following Python modules must be installed:

	•	paramiko (for SSH communication)
	•	pandas (for data handling and processing)
	•	pyyaml (for reading configuration files)
	•	logging (for application logging)
	•	os (for file system operations)
	•	stat (for file permission handling)


 Setting Up a Virtual Environment

To ensure isolation and avoid conflicts with system-wide packages:

	1.	Create a virtual environment:
 
 ```python3 -m venv /opt/ShredBackupSync/venv/```

 Logging

	•	The script automatically logs all operations to a file named <current_date>.log in the directory specified by log_path.
	•	Log levels, format, and permissions are configurable via the config.yaml file.

History Tracking

	•	The script uses the history_file specified in config.yaml to avoid reprocessing folders. If verification is needed, use the verify flag to force reprocessing.

For further assistance, feel free to reach out to the maintainer or consult the script’s inline documentation.
 
 
