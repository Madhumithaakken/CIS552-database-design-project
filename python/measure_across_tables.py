# measure_across_tables.py (final, non-normalized only)
import os
import time
import csv
import statistics
import socket
from dotenv import load_dotenv
import mysql.connector

# Load .env file (must be in the SAME folder as this script)
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '.env'))

cfg = {
    'host': os.getenv('MYSQL_HOST', 'localhost'),
    'user': os.getenv('MYSQL_USER'),
    'password': os.getenv('MYSQL_PASSWORD'),
    'database': os.getenv('MYSQL_DATABASE'),
    'allow_local_infile': True,
    'connect_timeout': 10
}

# Tables for raw CSV imports (non-normalized)
TABLES = [
    ("raw_data_1MB", "1"),
    ("raw_data_10MB", "10"),
    ("raw_data_100MB", "100")
]

# SQL queries for NON-normalized tables (exact matches, no LOWER/LIKE)
QUERIES = {
    'Q1': "SELECT DISTINCT PersonName, BirthDate FROM {table};",
    'Q2': "SELECT DISTINCT PersonName, SchoolName, SchoolCampus FROM {table} WHERE StillWorking = 'yes';",
    'Q3': ("SELECT PersonName, JobTitle FROM {table} "
           "WHERE JobTitle = 'Assistant Professor' "
           "AND SchoolName = 'University of Massachusetts' "
           "AND SchoolCampus = 'Dartmouth' "
           "AND StillWorking = 'yes';"),
    'Q4': ("SELECT SchoolCampus, COUNT(DISTINCT PersonID) AS num_people FROM {table} "
           "WHERE EarningsYear = (SELECT MAX(EarningsYear) FROM {table}) "
           "AND StillWorking = 'yes' GROUP BY SchoolCampus;"),
    'Q5': "SELECT PersonID, PersonName, SUM(Earnings) AS total_earnings FROM {table} GROUP BY PersonID, PersonName;"
}

# Timing settings
WARMUPS = 2
REPEATS = 7


def measure_query(cursor, sql):
    """Run warmups + timed runs and return median + all samples."""
    # warmups
    for _ in range(WARMUPS):
        cursor.execute(sql)
        cursor.fetchall()

    # timed runs
    times = []
    for _ in range(REPEATS):
        start = time.perf_counter()
        cursor.execute(sql)
        cursor.fetchall()
        times.append(time.perf_counter() - start)

    return statistics.median(times), times


def main():
    # Connect to MySQL
    try:
        conn = mysql.connector.connect(**cfg)
    except Exception as e:
        print("ERROR: Could not connect to MySQL. Check your .env and that MySQL is running.")
        print("Details:", e)
        return

    cur = conn.cursor(buffered=True)

    results = []
    run_timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    host = socket.gethostname()

    # Run queries on each table
    for table_name, size_label in TABLES:
        print(f"\n=== Running queries on {table_name} ({size_label}MB) ===")

        for qid, qtemplate in QUERIES.items():
            sql = qtemplate.format(table=table_name)

            try:
                median_time, sample_times = measure_query(cur, sql)
                print(f"{qid} → median: {median_time:.6f}s")
            except Exception as e:
                print(f"ERROR running {qid} on {table_name}: {e}")
                median_time, sample_times = None, []

            results.append({
                'table': table_name,
                'size_mb': size_label,
                'query': qid,
                'median_s': "" if median_time is None else f"{median_time:.6f}",
                'samples': "" if not sample_times else ";".join(f"{t:.6f}" for t in sample_times),
                'timestamp': run_timestamp,
                'host': host
            })

    cur.close()
    conn.close()

    # Save CSV in project root
    out_path = os.path.join(os.path.dirname(__file__), "..", "results_all_tables.csv")
    try:
        with open(out_path, "w", newline="") as f:
            fieldnames = ['table', 'size_mb', 'query', 'median_s', 'samples', 'timestamp', 'host']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for r in results:
                writer.writerow(r)
        print("\nSaved results to:", os.path.abspath(out_path))
    except Exception as e:
        print("ERROR writing results CSV:", e)


if __name__ == "__main__":
    main()