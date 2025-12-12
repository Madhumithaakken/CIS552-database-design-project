USE CIS552_PROJECT;

-- Q1: Person name and birthdate
SELECT DISTINCT p.PersonName, p.BirthDate
FROM Person p;

-- Q2: Working employees and their school
SELECT DISTINCT p.PersonName, s.SchoolName, s.SchoolCampus
FROM Person p, Employment e, School s
WHERE p.PersonID = e.PersonID
  AND e.SchoolID = s.SchoolID
  AND e.StillWorking = 'yes';

-- Q3: Assistant Professors at UMass Dartmouth
SELECT p.PersonName, j.JobTitle
FROM Person p, Employment e, Job j, School s
WHERE p.PersonID = e.PersonID
  AND e.JobID = j.JobID
  AND e.SchoolID = s.SchoolID
  AND j.JobTitle = 'Assistant Professor'
  AND s.SchoolName = 'University of Massachusetts'
  AND s.SchoolCampus = 'Dartmouth'
  AND e.StillWorking = 'yes';

-- Q4: Count people per campus for latest year
SELECT s.SchoolCampus, COUNT(DISTINCT p.PersonID) AS num_people
FROM Person p, Employment e, School s
WHERE p.PersonID = e.PersonID
  AND e.SchoolID = s.SchoolID
  AND e.EarningsYear = (SELECT MAX(EarningsYear) FROM Employment)
  AND e.StillWorking = 'yes'
GROUP BY s.SchoolCampus;

-- Q5: Total earnings per person
SELECT p.PersonID, p.PersonName, SUM(e.Earnings) AS total_earnings
FROM Person p, Employment e
WHERE p.PersonID = e.PersonID
GROUP BY p.PersonID, p.PersonName;
