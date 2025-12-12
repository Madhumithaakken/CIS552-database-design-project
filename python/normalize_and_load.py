import os
from dotenv import load_dotenv
import mysql.connector

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '.env'))

cfg = {
    'host': os.getenv('MYSQL_HOST'),
    'user': os.getenv('MYSQL_USER'),
    'password': os.getenv('MYSQL_PASSWORD'),
    'database': os.getenv('MYSQL_DATABASE'),
    'allow_local_infile': True
}

conn = mysql.connector.connect(**cfg)
cur = conn.cursor()

print("Starting normalization from raw_data_100MB...")

cur.execute("""
INSERT IGNORE INTO Person (PersonID, PersonName, BirthDate)
SELECT DISTINCT PersonID, PersonName, BirthDate
FROM raw_data_100MB;
""")
conn.commit()

cur.execute("""
INSERT IGNORE INTO School (SchoolID, SchoolName)
SELECT DISTINCT SchoolID, SchoolName
FROM raw_data_100MB;
""")
conn.commit()

cur.execute("""
INSERT IGNORE INTO Campus (SchoolID, CampusName)
SELECT DISTINCT SchoolID, SchoolCampus
FROM raw_data_100MB;
""")
conn.commit()

cur.execute("""
INSERT IGNORE INTO Department (DepartmentID, DepartmentName)
SELECT DISTINCT DepartmentID, DepartmentName
FROM raw_data_100MB;
""")
conn.commit()

cur.execute("""
INSERT IGNORE INTO Job (JobID, JobTitle)
SELECT DISTINCT JobID, JobTitle
FROM raw_data_100MB;
""")
conn.commit()

cur.execute("""
INSERT INTO Employment (PersonID, JobID, SchoolID, SchoolCampus, DepartmentID, StillWorking, Earnings, EarningsYear)
SELECT PersonID, JobID, SchoolID, SchoolCampus, DepartmentID, StillWorking, Earnings, EarningsYear
FROM raw_data_100MB;
""")
conn.commit()

print("Normalization and load complete.")

cur.close()
conn.close()
