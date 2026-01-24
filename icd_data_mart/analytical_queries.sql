-- =============================================================================
-- Analytical Queries for Patient Data Mart
-- Praktikum 4
-- =============================================================================

-- =============================================================================
-- 1. PATIENT ANALYSIS
-- =============================================================================

-- 1.1 Patient count by Age Group and Gender
SELECT 
    dp.AgeGroup,
    dp.Geschlecht,
    COUNT(DISTINCT dp.PatientKey) AS PatientCount
FROM Dim_Patient dp
WHERE dp.IsCurrent = TRUE
GROUP BY dp.AgeGroup, dp.Geschlecht
ORDER BY 
    CASE dp.AgeGroup 
        WHEN '0-4' THEN 1 WHEN '5-9' THEN 2 WHEN '10-14' THEN 3 WHEN '15-19' THEN 4
        WHEN '20-24' THEN 5 WHEN '25-29' THEN 6 WHEN '30-34' THEN 7 WHEN '35-39' THEN 8
        WHEN '40-44' THEN 9 WHEN '45-49' THEN 10 WHEN '50-54' THEN 11 WHEN '55-59' THEN 12
        WHEN '60-64' THEN 13 WHEN '65-69' THEN 14 WHEN '70-74' THEN 15 WHEN '75-79' THEN 16
        WHEN '80-84' THEN 17 WHEN '85-89' THEN 18 WHEN '90+' THEN 19
        ELSE 20
    END,
    dp.Geschlecht;

-- 1.2 Patients by Practice/Clinic
SELECT 
    pr.PraxisName,
    pr.PraxisType,
    pr.City,
    COUNT(DISTINCT dp.PatientKey) AS PatientCount
FROM Dim_Patient dp
JOIN Dim_Praxis pr ON dp.PatientSource = pr.PraxisSource
WHERE dp.IsCurrent = TRUE
GROUP BY pr.PraxisName, pr.PraxisType, pr.City
ORDER BY PatientCount DESC;

-- =============================================================================
-- 2. EXAMINATION ANALYSIS
-- =============================================================================

-- 2.1 Examinations per Month
SELECT 
    dd.Year,
    dd.MonthName,
    COUNT(fu.UntersuchungFactKey) AS ExaminationCount,
    COUNT(DISTINCT fu.PatientKey) AS UniquePatients
FROM Fact_Untersuchung fu
JOIN Dim_Date dd ON fu.DateKey = dd.DateKey
GROUP BY dd.Year, dd.MonthNumber, dd.MonthName
ORDER BY dd.Year, dd.MonthNumber;

-- 2.2 Examinations by Practice Type
SELECT 
    pr.PraxisType,
    COUNT(fu.UntersuchungFactKey) AS TotalExaminations,
    COUNT(DISTINCT fu.PatientKey) AS UniquePatients,
    ROUND(AVG(fu.Tensio), 2) AS AvgTensio,
    ROUND(AVG(fu.Refraktion), 2) AS AvgRefraktion,
    ROUND(AVG(fu.Visus), 2) AS AvgVisus
FROM Fact_Untersuchung fu
JOIN Dim_Praxis pr ON fu.PraxisKey = pr.PraxisKey
GROUP BY pr.PraxisType;

-- 2.3 Top 10 Practices by Examination Volume
SELECT 
    pr.PraxisName,
    pr.City,
    COUNT(fu.UntersuchungFactKey) AS ExaminationCount,
    COUNT(DISTINCT fu.PatientKey) AS UniquePatients,
    ROUND(COUNT(fu.UntersuchungFactKey)::NUMERIC / 
          COUNT(DISTINCT fu.PatientKey), 2) AS AvgExamsPerPatient
FROM Fact_Untersuchung fu
JOIN Dim_Praxis pr ON fu.PraxisKey = pr.PraxisKey
GROUP BY pr.PraxisName, pr.City
ORDER BY ExaminationCount DESC
LIMIT 10;

-- 2.4 Examination Trend by Day of Week
SELECT 
    dd.DayName,
    dd.DayOfWeek,
    COUNT(fu.UntersuchungFactKey) AS ExaminationCount
FROM Fact_Untersuchung fu
JOIN Dim_Date dd ON fu.DateKey = dd.DateKey
GROUP BY dd.DayName, dd.DayOfWeek
ORDER BY dd.DayOfWeek;

-- =============================================================================
-- 3. ICD CODE / DIAGNOSIS ANALYSIS
-- =============================================================================

-- 3.1 Top 20 Most Common Diagnoses
SELECT 
    dic.ICD_Code,
    dic.Code_Titel,
    dic.GruppeCode,
    dic.KapitelNr,
    COUNT(fu.UntersuchungFactKey) AS DiagnosisCount,
    COUNT(DISTINCT fu.PatientKey) AS AffectedPatients
FROM Fact_Untersuchung fu
JOIN Dim_ICD_Code dic ON fu.ICDKey = dic.ICDKey
WHERE dic.IsCurrent = TRUE AND fu.ICDKey IS NOT NULL
GROUP BY dic.ICD_Code, dic.Code_Titel, dic.GruppeCode, dic.KapitelNr
ORDER BY DiagnosisCount DESC
LIMIT 20;

-- 3.2 Diagnosis Distribution by ICD Chapter (Kapitel)
SELECT 
    dic.KapitelNr,
    dic.KapitelTitel,
    COUNT(fu.UntersuchungFactKey) AS DiagnosisCount,
    COUNT(DISTINCT fu.PatientKey) AS PatientCount,
    ROUND(COUNT(fu.UntersuchungFactKey)::NUMERIC / 
          (SELECT COUNT(*) FROM Fact_Untersuchung WHERE ICDKey IS NOT NULL) * 100, 2) AS Percentage
FROM Fact_Untersuchung fu
JOIN Dim_ICD_Code dic ON fu.ICDKey = dic.ICDKey
WHERE dic.IsCurrent = TRUE AND fu.ICDKey IS NOT NULL
GROUP BY dic.KapitelNr, dic.KapitelTitel
ORDER BY DiagnosisCount DESC;

-- 3.3 Diagnosis Distribution by ICD Group
SELECT 
    dic.GruppeCode,
    dic.GruppeTitel,
    dic.KapitelNr,
    COUNT(fu.UntersuchungFactKey) AS DiagnosisCount,
    COUNT(DISTINCT fu.PatientKey) AS PatientCount
FROM Fact_Untersuchung fu
JOIN Dim_ICD_Code dic ON fu.ICDKey = dic.ICDKey
WHERE dic.IsCurrent = TRUE AND fu.ICDKey IS NOT NULL
GROUP BY dic.GruppeCode, dic.GruppeTitel, dic.KapitelNr
ORDER BY DiagnosisCount DESC
LIMIT 20;

-- 3.4 Diagnosis by Age Group
SELECT 
    dp.AgeGroup,
    dic.KapitelNr,
    dic.KapitelTitel,
    COUNT(fu.UntersuchungFactKey) AS DiagnosisCount
FROM Fact_Untersuchung fu
JOIN Dim_Patient dp ON fu.PatientKey = dp.PatientKey
JOIN Dim_ICD_Code dic ON fu.ICDKey = dic.ICDKey
WHERE dp.IsCurrent = TRUE AND dic.IsCurrent = TRUE AND fu.ICDKey IS NOT NULL
GROUP BY dp.AgeGroup, dic.KapitelNr, dic.KapitelTitel
ORDER BY dp.AgeGroup, DiagnosisCount DESC;

-- =============================================================================
-- 4. MEASUREMENT ANALYSIS
-- =============================================================================

-- 4.1 Average Measurements by Practice
SELECT 
    pr.PraxisName,
    pr.PraxisType,
    COUNT(fu.UntersuchungFactKey) AS ExaminationsWithMeasurements,
    ROUND(AVG(fu.Tensio), 2) AS AvgTensio,
    ROUND(STDDEV(fu.Tensio), 2) AS StdDevTensio,
    ROUND(AVG(fu.Refraktion), 2) AS AvgRefraktion,
    ROUND(AVG(fu.Visus), 2) AS AvgVisus
FROM Fact_Untersuchung fu
JOIN Dim_Praxis pr ON fu.PraxisKey = pr.PraxisKey
WHERE fu.HasMeasurements = TRUE
GROUP BY pr.PraxisName, pr.PraxisType
ORDER BY ExaminationsWithMeasurements DESC;

-- 4.2 Measurement Trends Over Time
SELECT 
    dd.Year,
    dd.Quarter,
    COUNT(fu.UntersuchungFactKey) AS MeasurementCount,
    ROUND(AVG(fu.Tensio), 2) AS AvgTensio,
    ROUND(AVG(fu.Refraktion), 2) AS AvgRefraktion,
    ROUND(AVG(fu.Visus), 2) AS AvgVisus
FROM Fact_Untersuchung fu
JOIN Dim_Date dd ON fu.DateKey = dd.DateKey
WHERE fu.HasMeasurements = TRUE
GROUP BY dd.Year, dd.Quarter
ORDER BY dd.Year, dd.Quarter;

-- 4.3 Measurements by Age Group
SELECT 
    dp.AgeGroup,
    COUNT(fu.UntersuchungFactKey) AS MeasurementCount,
    ROUND(AVG(fu.Tensio), 2) AS AvgTensio,
    ROUND(AVG(fu.Refraktion), 2) AS AvgRefraktion,
    ROUND(AVG(fu.Visus), 2) AS AvgVisus
FROM Fact_Untersuchung fu
JOIN Dim_Patient dp ON fu.PatientKey = dp.PatientKey
WHERE fu.HasMeasurements = TRUE AND dp.IsCurrent = TRUE
GROUP BY dp.AgeGroup
ORDER BY 
    CASE dp.AgeGroup 
        WHEN '0-4' THEN 1 WHEN '5-9' THEN 2 WHEN '10-14' THEN 3 WHEN '15-19' THEN 4
        WHEN '20-24' THEN 5 WHEN '25-29' THEN 6 WHEN '30-34' THEN 7 WHEN '35-39' THEN 8
        WHEN '40-44' THEN 9 WHEN '45-49' THEN 10 WHEN '50-54' THEN 11 WHEN '55-59' THEN 12
        WHEN '60-64' THEN 13 WHEN '65-69' THEN 14 WHEN '70-74' THEN 15 WHEN '75-79' THEN 16
        WHEN '80-84' THEN 17 WHEN '85-89' THEN 18 WHEN '90+' THEN 19
        ELSE 20
    END;

-- =============================================================================
-- 5. ANAMNESE (MEDICAL HISTORY) ANALYSIS
-- =============================================================================

-- 5.1 Most Common Conditions in Patient History
SELECT 
    dic.ICD_Code,
    dic.Code_Titel,
    dic.KapitelNr,
    COUNT(fa.AnamneseFactKey) AS OccurrenceCount,
    COUNT(DISTINCT fa.PatientKey) AS PatientCount
FROM Fact_Patient_Anamnese fa
JOIN Dim_ICD_Code dic ON fa.ICDKey = dic.ICDKey
WHERE dic.IsCurrent = TRUE
GROUP BY dic.ICD_Code, dic.Code_Titel, dic.KapitelNr
ORDER BY OccurrenceCount DESC
LIMIT 20;

-- 5.2 Anamnese by Practice
SELECT 
    pr.PraxisName,
    pr.PraxisType,
    COUNT(fa.AnamneseFactKey) AS AnamneseRecords,
    COUNT(DISTINCT fa.PatientKey) AS PatientsWithHistory,
    ROUND(COUNT(fa.AnamneseFactKey)::NUMERIC / 
          COUNT(DISTINCT fa.PatientKey), 2) AS AvgConditionsPerPatient
FROM Fact_Patient_Anamnese fa
JOIN Dim_Praxis pr ON fa.PraxisKey = pr.PraxisKey
GROUP BY pr.PraxisName, pr.PraxisType
ORDER BY AnamneseRecords DESC;

-- =============================================================================
-- 6. REGIONAL ANALYSIS
-- =============================================================================

-- 6.1 Patient and Examination Distribution by City
SELECT 
    pr.City,
    pr.PraxisType,
    COUNT(DISTINCT dp.PatientKey) AS PatientCount,
    COUNT(fu.UntersuchungFactKey) AS ExaminationCount,
    COUNT(DISTINCT fu.ICDKey) AS UniqueDiagnoses
FROM Dim_Patient dp
LEFT JOIN Fact_Untersuchung fu ON dp.PatientKey = fu.PatientKey
JOIN Dim_Praxis pr ON dp.PatientSource = pr.PraxisSource
WHERE dp.IsCurrent = TRUE
GROUP BY pr.City, pr.PraxisType
ORDER BY PatientCount DESC;

-- 6.2 Comparison: University Clinics vs Practices
SELECT 
    pr.PraxisType,
    COUNT(DISTINCT dp.PatientKey) AS PatientCount,
    COUNT(fu.UntersuchungFactKey) AS ExaminationCount,
    SUM(CASE WHEN fu.HasMeasurements THEN 1 ELSE 0 END) AS WithMeasurements,
    ROUND(SUM(CASE WHEN fu.HasMeasurements THEN 1 ELSE 0 END)::NUMERIC / 
          NULLIF(COUNT(fu.UntersuchungFactKey), 0) * 100, 2) AS MeasurementRate
FROM Dim_Patient dp
LEFT JOIN Fact_Untersuchung fu ON dp.PatientKey = fu.PatientKey
JOIN Dim_Praxis pr ON dp.PatientSource = pr.PraxisSource
WHERE dp.IsCurrent = TRUE
GROUP BY pr.PraxisType;

-- =============================================================================
-- 7. COHORT / ADVANCED ANALYSIS
-- =============================================================================

-- 7.1 Patient Retention: Patients with Multiple Examinations
WITH PatientExamCounts AS (
    SELECT 
        fu.PatientKey,
        COUNT(fu.UntersuchungFactKey) AS ExamCount
    FROM Fact_Untersuchung fu
    GROUP BY fu.PatientKey
)
SELECT 
    CASE 
        WHEN ExamCount = 1 THEN '1 Exam'
        WHEN ExamCount BETWEEN 2 AND 3 THEN '2-3 Exams'
        WHEN ExamCount BETWEEN 4 AND 5 THEN '4-5 Exams'
        WHEN ExamCount BETWEEN 6 AND 10 THEN '6-10 Exams'
        ELSE '10+ Exams'
    END AS ExaminationBucket,
    COUNT(PatientKey) AS PatientCount,
    ROUND(COUNT(PatientKey)::NUMERIC / (SELECT COUNT(*) FROM PatientExamCounts) * 100, 2) AS Percentage
FROM PatientExamCounts
GROUP BY 
    CASE 
        WHEN ExamCount = 1 THEN '1 Exam'
        WHEN ExamCount BETWEEN 2 AND 3 THEN '2-3 Exams'
        WHEN ExamCount BETWEEN 4 AND 5 THEN '4-5 Exams'
        WHEN ExamCount BETWEEN 6 AND 10 THEN '6-10 Exams'
        ELSE '10+ Exams'
    END
ORDER BY MIN(ExamCount);

-- 7.2 Co-occurrence of Diagnoses (which ICD codes appear together)
SELECT 
    d1.ICD_Code AS Diagnosis1,
    d2.ICD_Code AS Diagnosis2,
    COUNT(*) AS CoOccurrenceCount
FROM Fact_Untersuchung fu1
JOIN Fact_Untersuchung fu2 ON fu1.PatientKey = fu2.PatientKey 
    AND fu1.ICDKey < fu2.ICDKey  -- Avoid duplicates and self-joins
JOIN Dim_ICD_Code d1 ON fu1.ICDKey = d1.ICDKey
JOIN Dim_ICD_Code d2 ON fu2.ICDKey = d2.ICDKey
WHERE fu1.ICDKey IS NOT NULL AND fu2.ICDKey IS NOT NULL
GROUP BY d1.ICD_Code, d2.ICD_Code
HAVING COUNT(*) > 5
ORDER BY CoOccurrenceCount DESC
LIMIT 20;

-- 7.3 Time Between Examinations per Patient
WITH ExamDates AS (
    SELECT 
        fu.PatientKey,
        dd.FullDate,
        LAG(dd.FullDate) OVER (PARTITION BY fu.PatientKey ORDER BY dd.FullDate) AS PrevExamDate
    FROM Fact_Untersuchung fu
    JOIN Dim_Date dd ON fu.DateKey = dd.DateKey
)
SELECT 
    ROUND(AVG(FullDate - PrevExamDate), 1) AS AvgDaysBetweenExams,
    MIN(FullDate - PrevExamDate) AS MinDays,
    MAX(FullDate - PrevExamDate) AS MaxDays,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY FullDate - PrevExamDate) AS MedianDays
FROM ExamDates
WHERE PrevExamDate IS NOT NULL;
