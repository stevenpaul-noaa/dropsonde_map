import csv
import sqlite3
from datetime import datetime

# === CONFIG ===
INPUT_FILE = r'G:\Shared drives\OMAO AOC Mission Instrumentation\Instruments & Systems\AVAPS\Software\sonde_counter_tail_nc\counter_all_drops_tail.txt'
DB_PATH = r'instance/dropsonde.db'

def process_and_update():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    with open(INPUT_FILE, 'r', newline='') as infile:
        reader = csv.reader(infile)
        for row in reader:
            uid = row[0]
            try:
                droptime_str = uid.replace('_', 'T')
                droptime = datetime.strptime(droptime_str, "%Y%m%dT%H%M%S")
            except ValueError:
                continue

            operator = row[3]
            serial = row[5]
            lat = float(row[8])
            lon = float(row[9])
            tail = row[10].strip()

            cur.execute("""
                INSERT OR IGNORE INTO dropsonde_data (uid, operator, serial, lat, lon, tail, droptime)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (uid, operator, serial, lat, lon, tail, droptime.isoformat()))

    conn.commit()
    cur.close()
    conn.close()
    print("Database update complete.")

if __name__ == "__main__":
    process_and_update()