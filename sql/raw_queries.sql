USE CIS552_PROJECT;

-- Q1: Person name and birthdate
SELECT DISTINCT PersonName, BirthDate
FROM raw_data_1MB;

-- Q2: Working employees and their school
SELECT DISTINCT PersonName, SchoolName, SchoolCampus
FROM raw_data_1MB
WHERE StillWorking = 'yes';

-- Q3: Assistant Professors at UMass Dartmouth
SELECT PersonName, JobTitle
FROM raw_data_1MB
WHERE JobTitle = 'Assistant Professor'
  AND SchoolName = 'University of Massachusetts'
  AND SchoolCampus = 'Dartmouth'
  AND StillWorking = 'yes';

-- Q4: Count people per campus for latest year
SELECT SchoolCampus, COUNT(DISTINCT PersonID) AS num_people
FROM raw_data_1MB
WHERE EarningsYear = (SELECT MAX(EarningsYear) FROM raw_data_1MB)
  AND StillWorking = 'yes'
GROUP BY SchoolCampus;

-- Q5: Total earnings per person
SELECT PersonID, PersonName, SUM(Earnings) AS total_earnings
FROM raw_data_1MB
GROUP BY PersonID, PersonName;
