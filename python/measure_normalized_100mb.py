# measure_normalized_100mb.py
# FINAL version: optimized normalized queries + safe index creation
# Uses ONLY FROM + WHERE (implicit joins)

print("SCRIPT STARTED")

import os
import time
import csv
import statistics
import socket
from dotenv import load_dotenv
import mysql.connector

# Load .env (must be in the SAME folder)
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '.env'))

cfg = {
    'host': os.getenv('MYSQL_HOST', 'localhost'),
    'user': os.getenv('MYSQL_USER'),
    'password': os.getenv('MYSQL_PASSWORD'),
    'database': os.getenv('MYSQL_DATABASE'),
    'connect_timeout': 10
}

WARMUPS = 2
REPEATS = 7

# ==========================
# OPTIMIZED NORMALIZED QUERIES
# (implicit joins only)
# ==========================
QUERIES = {

    # Q1
    'Q1': (
        "SELECT PersonName, BirthDate "
        "FROM Person;"
    ),

    # Q2
    'Q2': (
        "SELECT DISTINCT p.PersonName, s.SchoolName, e.SchoolCampus "
        "FROM Employment e, Person p, School s "
        "WHERE e.PersonID = p.PersonID "
        "AND e.SchoolID = s.SchoolID "
        "AND e.StillWorking = 'yes';"
    ),

    # Q3
   'Q3': (
    "SELECT p.PersonName, j.JobTitle "
    "FROM Employment e, Person p, Job j, School s "
    "WHERE e.PersonID = p.PersonID "
    "AND e.JobID = j.JobID "
    "AND e.SchoolID = s.SchoolID "
    "AND e.StillWorking = 'yes' "
    "AND j.JobTitle = 'Assistant Professor' "
    "AND s.SchoolName = 'University of Massachusetts' "
    "AND e.SchoolCampus = 'Dartmouth';"
),
    # Q4
    'Q4': (
        "SELECT e.SchoolCampus, COUNT(DISTINCT e.PersonID) AS num_people "
        "FROM Employment e "
        "WHERE e.StillWorking = 'yes' "
        "AND e.EarningsYear = (SELECT MAX(EarningsYear) FROM Employment) "
        "GROUP BY e.SchoolCampus;"
    ),

    # Q5 (aggregate first, then join)
    'Q5': (
        "SELECT p.PersonID, p.PersonName, t.total_earnings "
        "FROM Person p, "
        "     (SELECT PersonID, SUM(Earnings) AS total_earnings "
        "      FROM Employment "
        "      GROUP BY PersonID) t "
        "WHERE p.PersonID = t.PersonID;"
    )
}

# ==========================
# SAFE INDEX CREATION
# ==========================
INDEX_SQL = [
    "ALTER TABLE Employment ADD INDEX idx_emp_personid (PersonID)",
    "ALTER TABLE Employment ADD INDEX idx_emp_schoolid (SchoolID)",
    "ALTER TABLE Employment ADD INDEX idx_emp_earningsyear (EarningsYear)",
    "ALTER TABLE Employment ADD INDEX idx_emp_personid_earn (PersonID, Earnings)",
    "ALTER TABLE Employment ADD INDEX idx_emp_stillworking (StillWorking)",
    "ALTER TABLE Job ADD INDEX idx_job_title (JobTitle(100))",
    "ALTER TABLE School ADD INDEX idx_school_name (SchoolName(100))"
]

def measure_query(cursor, sql):
    # warm-up runs
    for _ in range(WARMUPS):
        cursor.execute(sql)
        cursor.fetchall()

    times = []
    for _ in range(REPEATS):
        start = time.perf_counter()
        cursor.execute(sql)
        cursor.fetchall()
        times.append(time.perf_counter() - start)

    return statistics.median(times), times


def main():
    try:
        conn = mysql.connector.connect(**cfg)
    except Exception as e:
        print("ERROR: Cannot connect to MySQL")
        print(e)
        return

    cur = conn.cursor(buffered=True)

    print("\n=== Creating indexes (safe mode) ===")
    for stmt in INDEX_SQL:
        try:
            cur.execute(stmt)
            print("Created:", stmt)
        except Exception as e:
            print("Index warning (already exists or skipped):", e)
    conn.commit()
    print("Index step completed.\n")

    results = []
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    host = socket.gethostname()

    print("=== Running OPTIMIZED NORMALIZED queries (100MB) ===")

    for qid, sql in QUERIES.items():
        print(f"Running {qid}")
        try:
            median_time, samples = measure_query(cur, sql)
            print(f"{qid} → median: {median_time:.6f}s")
        except Exception as e:
            print(f"ERROR in {qid}:", e)
            median_time, samples = None, []

        results.append({
            'query': qid,
            'median_s': "" if median_time is None else f"{median_time:.6f}",
            'samples': "" if not samples else ";".join(f"{t:.6f}" for t in samples),
            'timestamp': timestamp,
            'host': host
        })

    cur.close()
    conn.close()

    out_path = os.path.join(os.path.dirname(__file__), "..", "results_normalized_100MB.csv")
    with open(out_path, "w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=['query', 'median_s', 'samples', 'timestamp', 'host']
        )
        writer.writeheader()
        for r in results:
            writer.writerow(r)

    print("\nSaved results to:", os.path.abspath(out_path))


if __name__ == "__main__":
    main()
