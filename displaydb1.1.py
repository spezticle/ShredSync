#!/spindles/shred/.venv/shredsync/bin/python
# displaydb1.1.py

import sqlite3
from tabulate import tabulate

DB_FILE = "shredsync.db"

def fetch_first_three_entries():
    """Fetch the first three entries from the shredmatch table."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    try:
        cursor.execute("""
        SELECT unique_id, timestamp_inserted, shredded_timestamp, shredded_source_folder,
               import_source_folder, shredlog_source_folder, shredded_UUID, import_UUID,
               import_folder_name, matching_log_filename, shredded_files, wholename
        FROM shredmatch
        ORDER BY timestamp_inserted ASC
        LIMIT 3
        """)
        rows = cursor.fetchall()
        conn.close()
        return rows
    except sqlite3.OperationalError as e:
        conn.close()
        print(f"Error fetching data: {e}")
        return None


def display_entries(entries):
    """Display the entries in a tabular friendly format."""
    if not entries:
        print("No entries found in the database.")
        return

    headers = [
        "Unique ID", "Inserted Timestamp", "Shredded Timestamp", "Shredded Source",
        "Import Source", "Shred Log Source", "Shredded UUID", "Import UUID",
        "Import Folder", "Log Filename", "Shredded Files", "Wholename"
    ]

    # Format shredded files for better readability
    formatted_entries = []
    for entry in entries:
        formatted_entry = list(entry)
        formatted_entry[10] = formatted_entry[10].replace(", ", "\n")  # Format shredded files into multiple lines
        formatted_entries.append(formatted_entry)

    print(tabulate(formatted_entries, headers=headers, tablefmt="grid"))


def main():
    entries = fetch_first_three_entries()
    display_entries(entries)


if __name__ == "__main__":
    main()
