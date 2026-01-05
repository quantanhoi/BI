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
        WHEN '0-17' THEN 1
        WHEN '18-30' THEN 2
        WHEN '31-45' THEN 3
        WHEN '46-60' THEN 4
        WHEN '61-75' THEN 5
        WHEN '76+' THEN 6
        ELSE 7
    END,
    dp.Geschlecht;

-- 1.2 Patients with Master Records (unified across practices)
SELECT 
    COUNT(DISTINCT dp.PatientKey) AS TotalPatients,
    SUM(CASE WHEN dp.IsMasterRecord THEN 1 ELSE 0 END) AS WithMasterRecord,
    SUM(CASE WHEN NOT dp.IsMasterRecord THEN 1 ELSE 0 END) AS WithoutMasterRecord,
    ROUND(SUM(CASE WHEN dp.IsMasterRecord THEN 1 ELSE 0 END)::NUMERIC / 
          COUNT(DISTINCT dp.PatientKey) * 100, 2) AS MasterRecordPercentage
FROM Dim_Patient dp
WHERE dp.IsCurrent = TRUE;

-- 1.3 Patients by Practice/Clinic
SELECT 
    pr.PraxisName,
    pr.PraxisType,
    pr.City,
    pr.Region,
    COUNT(DISTINCT dp.PatientKey) AS PatientCount
FROM Dim_Patient dp
JOIN Dim_Praxis pr ON dp.PatientSource = pr.PraxisSource
WHERE dp.IsCurrent = TRUE
GROUP BY pr.PraxisName, pr.PraxisType, pr.City, pr.Region
ORDER BY PatientCount DESC;

-- =============================================================================
-- 2. EXAMINATION ANALYSIS
-- =============================================================================

-- 2.1 Examinations per Month
SELECT 
    dd.Year,
    dd.MonthName,
    COUNT(fu.UntersuchungFactKey) AS ExaminationCount,
    COUNT(DISTINCT fu.PatientKey) AS UniquePatients,
    ROUND(AVG(fu.DiagnoseCount), 2) AS AvgDiagnosesPerExam
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
    COUNT(fu.UntersuchungFactKey) AS ExaminationCount,
    ROUND(AVG(fu.DiagnoseCount), 2) AS AvgDiagnoses
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
        WHEN '0-17' THEN 1
        WHEN '18-30' THEN 2
        WHEN '31-45' THEN 3
        WHEN '46-60' THEN 4
        WHEN '61-75' THEN 5
        WHEN '76+' THEN 6
        ELSE 7
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

-- 6.1 Patient and Examination Distribution by Region
SELECT 
    pr.Region,
    COUNT(DISTINCT dp.PatientKey) AS PatientCount,
    COUNT(fu.UntersuchungFactKey) AS ExaminationCount,
    COUNT(DISTINCT fu.ICDKey) AS UniqueDiagnoses,
    ROUND(AVG(fu.DiagnoseCount), 2) AS AvgDiagnosesPerExam
FROM Dim_Patient dp
LEFT JOIN Fact_Untersuchung fu ON dp.PatientKey = fu.PatientKey
JOIN Dim_Praxis pr ON dp.PatientSource = pr.PraxisSource
WHERE dp.IsCurrent = TRUE
GROUP BY pr.Region
ORDER BY PatientCount DESC;

-- 6.2 Comparison: University Clinics vs Practices
SELECT 
    pr.PraxisType,
    COUNT(DISTINCT dp.PatientKey) AS PatientCount,
    COUNT(fu.UntersuchungFactKey) AS ExaminationCount,
    ROUND(AVG(fu.DiagnoseCount), 2) AS AvgDiagnoses,
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
