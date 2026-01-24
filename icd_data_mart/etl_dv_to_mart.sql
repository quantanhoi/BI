-- ETL: Data Vault to Data Mart
-- Praktikum 4



-- STEP 1: Populate Dim_Date
-- Generate date dimension for a range of years


INSERT INTO Dim_Date (
    DateKey, FullDate, DayOfWeek, DayName, DayOfMonth, DayOfYear,
    WeekOfYear, MonthNumber, MonthName, Quarter, Year, IsWeekend
)
SELECT 
    TO_CHAR(datum, 'YYYYMMDD')::INT AS DateKey,
    datum AS FullDate,
    EXTRACT(DOW FROM datum)::INT AS DayOfWeek,
    TO_CHAR(datum, 'Day') AS DayName,
    EXTRACT(DAY FROM datum)::INT AS DayOfMonth,
    EXTRACT(DOY FROM datum)::INT AS DayOfYear,
    EXTRACT(WEEK FROM datum)::INT AS WeekOfYear,
    EXTRACT(MONTH FROM datum)::INT AS MonthNumber,
    TO_CHAR(datum, 'Month') AS MonthName,
    EXTRACT(QUARTER FROM datum)::INT AS Quarter,
    EXTRACT(YEAR FROM datum)::INT AS Year,
    CASE WHEN EXTRACT(DOW FROM datum) IN (0, 6) THEN TRUE ELSE FALSE END AS IsWeekend
FROM (
    SELECT generate_series('2015-01-01'::DATE, '2030-12-31'::DATE, '1 day'::INTERVAL)::DATE AS datum
) dates
ON CONFLICT (DateKey) DO NOTHING;


-- STEP 2: Populate Dim_Praxis
-- Extract unique practice/clinic sources from Data Vault


INSERT INTO Dim_Praxis (PraxisSource, PraxisName, PraxisType, City)
SELECT DISTINCT
    hp.PatientSource AS PraxisSource,
    -- Extract readable name from source
    CASE 
        WHEN hp.PatientSource LIKE 'praxis_%' THEN 
            'Praxis ' || INITCAP(REPLACE(SUBSTRING(hp.PatientSource FROM 8), '_', ' '))
        WHEN hp.PatientSource LIKE 'uniklinik_%' THEN 
            'Universitätsklinikum ' || INITCAP(REPLACE(SUBSTRING(hp.PatientSource FROM 11), '_', ' '))
        ELSE hp.PatientSource
    END AS PraxisName,
    CASE 
        WHEN hp.PatientSource LIKE 'uniklinik_%' THEN 'Uniklinik'
        ELSE 'Praxis'
    END AS PraxisType,
    -- Extract city name
    CASE 
        WHEN hp.PatientSource LIKE 'praxis_%' THEN 
            INITCAP(REPLACE(SUBSTRING(hp.PatientSource FROM 8), '_', ' '))
        WHEN hp.PatientSource LIKE 'uniklinik_%' THEN 
            INITCAP(REPLACE(SUBSTRING(hp.PatientSource FROM 11), '_', ' '))
        ELSE NULL
    END AS City
FROM Hub_Patient hp
ON CONFLICT (PraxisSource) DO NOTHING;


-- STEP 3: Populate Dim_ICD_Code
-- Denormalized ICD hierarchy from Data Vault


INSERT INTO Dim_ICD_Code (
    HK_ICD_Code, ICD_Code, Code_Titel, GruppeCode, GruppeTitel, 
    KapitelNr, KapitelTitel, EffectiveDate, IsCurrent
)
SELECT 
    hc.HK_ICD_Code,
    hc.ICD_Code,
    sc.Code_Titel,
    hg.GruppeCode,
    sg.GruppeTitel,
    hk.KapitelNr,
    sk.KapitelTitel,
    CURRENT_DATE AS EffectiveDate,
    TRUE AS IsCurrent
FROM Hub_ICD_Code hc
-- Get latest satellite data for code
LEFT JOIN LATERAL (
    SELECT Code_Titel
    FROM Sat_ICD_Code
    WHERE HK_ICD_Code = hc.HK_ICD_Code
    ORDER BY LoadDate DESC
    LIMIT 1
) sc ON TRUE
-- Join to Gruppe via Link
LEFT JOIN Link_Gruppe_Code lgc ON hc.HK_ICD_Code = lgc.HK_ICD_Code
LEFT JOIN Hub_ICD_Gruppe hg ON lgc.HK_ICD_Gruppe = hg.HK_ICD_Gruppe
LEFT JOIN LATERAL (
    SELECT GruppeTitel
    FROM Sat_ICD_Gruppe
    WHERE HK_ICD_Gruppe = hg.HK_ICD_Gruppe
    ORDER BY LoadDate DESC
    LIMIT 1
) sg ON TRUE
-- Join to Kapitel via Link
LEFT JOIN Link_Kapitel_Gruppe lkg ON hg.HK_ICD_Gruppe = lkg.HK_ICD_Gruppe
LEFT JOIN Hub_ICD_Kapitel hk ON lkg.HK_ICD_Kapitel = hk.HK_ICD_Kapitel
LEFT JOIN LATERAL (
    SELECT KapitelTitel
    FROM Sat_ICD_Kapitel
    WHERE HK_ICD_Kapitel = hk.HK_ICD_Kapitel
    ORDER BY LoadDate DESC
    LIMIT 1
) sk ON TRUE
ON CONFLICT DO NOTHING;


-- STEP 4: Populate Dim_Patient
-- Patient dimension with master data when available


INSERT INTO Dim_Patient (
    HK_Patient, PatientID, PatientSource,
    Nachname, Vorname, Geburtsdatum, Versicherung, Geschlecht, AgeGroup, Age,
    EffectiveDate, IsCurrent
)
SELECT 
    hp.HK_Patient,
    hp.PatientID,
    hp.PatientSource,
    sps.Nachname,
    sps.Vorname,
    sps.Geburtsdatum,
    sps.Versicherung,
    sps.Geschlecht,
    -- Calculate age group in 5-year blocks
    CASE 
        WHEN sps.Geburtsdatum IS NULL THEN 'Unbekannt'
        WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, sps.Geburtsdatum)) < 5 THEN '0-4'
        WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, sps.Geburtsdatum)) < 10 THEN '5-9'
        WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, sps.Geburtsdatum)) < 15 THEN '10-14'
        WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, sps.Geburtsdatum)) < 20 THEN '15-19'
        WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, sps.Geburtsdatum)) < 25 THEN '20-24'
        WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, sps.Geburtsdatum)) < 30 THEN '25-29'
        WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, sps.Geburtsdatum)) < 35 THEN '30-34'
        WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, sps.Geburtsdatum)) < 40 THEN '35-39'
        WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, sps.Geburtsdatum)) < 45 THEN '40-44'
        WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, sps.Geburtsdatum)) < 50 THEN '45-49'
        WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, sps.Geburtsdatum)) < 55 THEN '50-54'
        WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, sps.Geburtsdatum)) < 60 THEN '55-59'
        WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, sps.Geburtsdatum)) < 65 THEN '60-64'
        WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, sps.Geburtsdatum)) < 70 THEN '65-69'
        WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, sps.Geburtsdatum)) < 75 THEN '70-74'
        WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, sps.Geburtsdatum)) < 80 THEN '75-79'
        WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, sps.Geburtsdatum)) < 85 THEN '80-84'
        WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, sps.Geburtsdatum)) < 90 THEN '85-89'
        ELSE '90+'
    END AS AgeGroup,
    EXTRACT(YEAR FROM AGE(CURRENT_DATE, sps.Geburtsdatum))::INT AS Age,
    CURRENT_DATE AS EffectiveDate,
    TRUE AS IsCurrent
FROM Hub_Patient hp
-- Get latest patient satellite data
LEFT JOIN LATERAL (
    SELECT Nachname, Vorname, Geburtsdatum, Versicherung, Geschlecht
    FROM Sat_Patient_Stammdaten
    WHERE HK_Patient = hp.HK_Patient
    ORDER BY LoadDate DESC
    LIMIT 1
) sps ON TRUE
ON CONFLICT DO NOTHING;


-- STEP 5: Populate Fact_Untersuchung
-- Examination facts with measurements and diagnoses


INSERT INTO Fact_Untersuchung (
    HK_Untersuchung, PatientKey, ICDKey, DateKey, PraxisKey,
    Untersuchungsdatum, Tensio, Refraktion, Visus,
    IsFirstVisit, HasMeasurements
)
SELECT 
    hu.HK_Untersuchung,
    dp.PatientKey,
    dic.ICDKey,
    dd.DateKey,
    dpr.PraxisKey,
    su.Untersuchungsdatum,
    sm.Tensio,
    sm.Refraktion,
    sm.Visus,
    -- Mark as first visit if this is the earliest examination for this patient
    CASE WHEN su.Untersuchungsdatum = first_visit.FirstVisitDate THEN TRUE ELSE FALSE END AS IsFirstVisit,
    CASE WHEN sm.Tensio IS NOT NULL OR sm.Refraktion IS NOT NULL OR sm.Visus IS NOT NULL 
         THEN TRUE ELSE FALSE END AS HasMeasurements
FROM Hub_Untersuchung hu
-- Link to Patient
JOIN Link_Patient_Untersuchung lpu ON hu.HK_Untersuchung = lpu.HK_Untersuchung
JOIN Hub_Patient hp ON lpu.HK_Patient = hp.HK_Patient
JOIN Dim_Patient dp ON hp.HK_Patient = dp.HK_Patient AND dp.IsCurrent = TRUE
-- Get examination satellite data
LEFT JOIN LATERAL (
    SELECT Untersuchungsdatum
    FROM Sat_Untersuchung
    WHERE HK_Untersuchung = hu.HK_Untersuchung
    ORDER BY LoadDate DESC
    LIMIT 1
) su ON TRUE
-- Get measurements
LEFT JOIN LATERAL (
    SELECT Tensio, Refraktion, Visus
    FROM Sat_Messwerte
    WHERE HK_Untersuchung = hu.HK_Untersuchung
    ORDER BY LoadDate DESC
    LIMIT 1
) sm ON TRUE
-- Calculate first visit date for each patient
LEFT JOIN LATERAL (
    SELECT MIN(su_inner.Untersuchungsdatum) AS FirstVisitDate
    FROM Link_Patient_Untersuchung lpu_inner
    JOIN Hub_Untersuchung hu_inner ON lpu_inner.HK_Untersuchung = hu_inner.HK_Untersuchung
    JOIN LATERAL (
        SELECT Untersuchungsdatum
        FROM Sat_Untersuchung
        WHERE HK_Untersuchung = hu_inner.HK_Untersuchung
        ORDER BY LoadDate DESC
        LIMIT 1
    ) su_inner ON TRUE
    WHERE lpu_inner.HK_Patient = hp.HK_Patient
    AND su_inner.Untersuchungsdatum IS NOT NULL
) first_visit ON TRUE
-- Link to Date dimension
LEFT JOIN Dim_Date dd ON TO_CHAR(su.Untersuchungsdatum, 'YYYYMMDD')::INT = dd.DateKey
-- Link to Praxis dimension
JOIN Dim_Praxis dpr ON hp.PatientSource = dpr.PraxisSource
-- Link to ICD Code (may have multiple diagnoses - we take the first one)
LEFT JOIN LATERAL (
    SELECT lud.HK_ICD_Code
    FROM Link_Untersuchung_Diagnose lud
    WHERE lud.HK_Untersuchung = hu.HK_Untersuchung
    ORDER BY lud.LoadDate
    LIMIT 1
) first_diag ON TRUE
LEFT JOIN Dim_ICD_Code dic ON first_diag.HK_ICD_Code = dic.HK_ICD_Code AND dic.IsCurrent = TRUE
WHERE su.Untersuchungsdatum IS NOT NULL;


-- STEP 6: Populate Fact_Patient_Anamnese
-- Patient medical history from Link_Patient_Anamnese


INSERT INTO Fact_Patient_Anamnese (
    PatientKey, ICDKey, PraxisKey, HK_Patient, HK_ICD_Code
)
SELECT 
    dp.PatientKey,
    dic.ICDKey,
    dpr.PraxisKey,
    lpa.HK_Patient,
    lpa.HK_ICD_Code
FROM Link_Patient_Anamnese lpa
JOIN Hub_Patient hp ON lpa.HK_Patient = hp.HK_Patient
JOIN Dim_Patient dp ON hp.HK_Patient = dp.HK_Patient AND dp.IsCurrent = TRUE
JOIN Hub_ICD_Code hic ON lpa.HK_ICD_Code = hic.HK_ICD_Code
JOIN Dim_ICD_Code dic ON hic.HK_ICD_Code = dic.HK_ICD_Code AND dic.IsCurrent = TRUE
JOIN Dim_Praxis dpr ON hp.PatientSource = dpr.PraxisSource
ON CONFLICT DO NOTHING;


-- Verification Queries


-- Check dimension counts
SELECT 'Dim_Date' AS TableName, COUNT(*) AS RowCount FROM Dim_Date
UNION ALL
SELECT 'Dim_Praxis', COUNT(*) FROM Dim_Praxis
UNION ALL
SELECT 'Dim_Patient', COUNT(*) FROM Dim_Patient
UNION ALL
SELECT 'Dim_ICD_Code', COUNT(*) FROM Dim_ICD_Code
UNION ALL
SELECT 'Fact_Untersuchung', COUNT(*) FROM Fact_Untersuchung
UNION ALL
SELECT 'Fact_Patient_Anamnese', COUNT(*) FROM Fact_Patient_Anamnese;
