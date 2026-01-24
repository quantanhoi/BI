-- =============================================================================
-- Patient Data Mart Schema - Praktikum 4
-- Dimensional Model for Patient/ICD Analytics
-- =============================================================================

-- Drop existing tables in reverse order (facts first, then dimensions)
DROP TABLE IF EXISTS Fact_Patient_Anamnese CASCADE;
DROP TABLE IF EXISTS Fact_Untersuchung CASCADE;
DROP TABLE IF EXISTS Dim_Patient CASCADE;
DROP TABLE IF EXISTS Dim_ICD_Code CASCADE;
DROP TABLE IF EXISTS Dim_Date CASCADE;
DROP TABLE IF EXISTS Dim_Praxis CASCADE;

-- =============================================================================
-- Dimension Tables
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Dim_Date - Date Dimension for time-based analysis
-- -----------------------------------------------------------------------------
CREATE TABLE Dim_Date (
    DateKey INT PRIMARY KEY,
    FullDate DATE NOT NULL UNIQUE,
    DayOfWeek INT NOT NULL,
    DayName VARCHAR(20) NOT NULL,
    DayOfMonth INT NOT NULL,
    DayOfYear INT NOT NULL,
    WeekOfYear INT NOT NULL,
    MonthNumber INT NOT NULL,
    MonthName VARCHAR(20) NOT NULL,
    Quarter INT NOT NULL,
    Year INT NOT NULL,
    IsWeekend BOOLEAN NOT NULL
);

-- -----------------------------------------------------------------------------
-- Dim_Praxis - Practice/Clinic Dimension
-- -----------------------------------------------------------------------------
CREATE TABLE Dim_Praxis (
    PraxisKey SERIAL PRIMARY KEY,
    PraxisSource VARCHAR(100) NOT NULL UNIQUE,
    PraxisName VARCHAR(200) NOT NULL,
    PraxisType VARCHAR(50) NOT NULL,  -- 'Praxis' or 'Uniklinik'
    City VARCHAR(100),
    LoadDate TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- -----------------------------------------------------------------------------
-- Dim_Patient - Patient Dimension
-- Note: AgeGroup should be 5-year blocks per Aufgabe 2 requirement
-- ETL should calculate: '0-4', '5-9', '10-14', ..., '85-89', '90+'
-- -----------------------------------------------------------------------------
CREATE TABLE Dim_Patient (
    PatientKey SERIAL PRIMARY KEY,
    HK_Patient CHAR(32) NOT NULL,
    PatientID VARCHAR(50) NOT NULL,
    PatientSource VARCHAR(50) NOT NULL,
    Nachname VARCHAR(100),
    Vorname VARCHAR(100),
    Geburtsdatum DATE,
    Versicherung VARCHAR(50),
    Geschlecht VARCHAR(10),
    AgeGroup VARCHAR(20),                   -- 5-year blocks: '0-4', '5-9', '10-14', etc.
    Age INT,                                -- Actual age for calculations
    EffectiveDate DATE NOT NULL,
    ExpirationDate DATE DEFAULT '9999-12-31',
    IsCurrent BOOLEAN DEFAULT TRUE,
    LoadDate TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_patient_hk ON Dim_Patient(HK_Patient);
CREATE INDEX idx_dim_patient_current ON Dim_Patient(IsCurrent);
CREATE INDEX idx_dim_patient_agegroup ON Dim_Patient(AgeGroup);

-- -----------------------------------------------------------------------------
-- Dim_ICD_Code - ICD Code Dimension with hierarchy (denormalized)
-- -----------------------------------------------------------------------------
CREATE TABLE Dim_ICD_Code (
    ICDKey SERIAL PRIMARY KEY,
    HK_ICD_Code CHAR(32) NOT NULL,
    ICD_Code VARCHAR(20) NOT NULL,
    Code_Titel VARCHAR(255),
    GruppeCode VARCHAR(20),
    GruppeTitel VARCHAR(255),
    KapitelNr VARCHAR(10),
    KapitelTitel VARCHAR(255),
    EffectiveDate DATE NOT NULL,
    ExpirationDate DATE DEFAULT '9999-12-31',
    IsCurrent BOOLEAN DEFAULT TRUE,
    LoadDate TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_icd_code_hk ON Dim_ICD_Code(HK_ICD_Code);
CREATE INDEX idx_dim_icd_code_current ON Dim_ICD_Code(IsCurrent);
CREATE INDEX idx_dim_icd_kapitel ON Dim_ICD_Code(KapitelNr);
CREATE INDEX idx_dim_icd_gruppe ON Dim_ICD_Code(GruppeCode);

-- =============================================================================
-- Fact Tables
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Fact_Untersuchung - Examination Facts with measurements
-- Note: Removed Untersuchungsart and Untersucher (no data in source)
-- Added IsFirstVisit for Aufgabe 2c requirement
-- -----------------------------------------------------------------------------
CREATE TABLE Fact_Untersuchung (
    UntersuchungFactKey SERIAL PRIMARY KEY,
    HK_Untersuchung CHAR(32) NOT NULL,
    PatientKey INT NOT NULL REFERENCES Dim_Patient(PatientKey),
    ICDKey INT REFERENCES Dim_ICD_Code(ICDKey),
    DateKey INT NOT NULL REFERENCES Dim_Date(DateKey),
    PraxisKey INT NOT NULL REFERENCES Dim_Praxis(PraxisKey),
    
    -- Date for reference
    Untersuchungsdatum DATE,
    
    -- Measures from Sat_Messwerte
    Tensio NUMERIC(10, 2),
    Refraktion NUMERIC(10, 2),
    Visus NUMERIC(10, 2),
    
    -- For Aufgabe 2c: "ersten Wert" - first visit only
    IsFirstVisit BOOLEAN DEFAULT FALSE,
    
    -- Derived measures
    HasMeasurements BOOLEAN DEFAULT FALSE,
    
    LoadDate TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_fact_untersuchung_patient ON Fact_Untersuchung(PatientKey);
CREATE INDEX idx_fact_untersuchung_date ON Fact_Untersuchung(DateKey);
CREATE INDEX idx_fact_untersuchung_praxis ON Fact_Untersuchung(PraxisKey);
CREATE INDEX idx_fact_untersuchung_icd ON Fact_Untersuchung(ICDKey);
CREATE INDEX idx_fact_untersuchung_hk ON Fact_Untersuchung(HK_Untersuchung);
CREATE INDEX idx_fact_untersuchung_first ON Fact_Untersuchung(IsFirstVisit);

-- -----------------------------------------------------------------------------
-- Fact_Patient_Anamnese - Patient Medical History Facts
-- Note: No DateKey because anamnese data has no dates
-- -----------------------------------------------------------------------------
CREATE TABLE Fact_Patient_Anamnese (
    AnamneseFactKey SERIAL PRIMARY KEY,
    PatientKey INT NOT NULL REFERENCES Dim_Patient(PatientKey),
    ICDKey INT NOT NULL REFERENCES Dim_ICD_Code(ICDKey),
    PraxisKey INT NOT NULL REFERENCES Dim_Praxis(PraxisKey),
    HK_Patient CHAR(32) NOT NULL,
    HK_ICD_Code CHAR(32) NOT NULL,
    LoadDate TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_fact_anamnese_patient ON Fact_Patient_Anamnese(PatientKey);
CREATE INDEX idx_fact_anamnese_icd ON Fact_Patient_Anamnese(ICDKey);
CREATE INDEX idx_fact_anamnese_praxis ON Fact_Patient_Anamnese(PraxisKey);

-- =============================================================================
-- Views for Analysis
-- =============================================================================

-- View: First Visit Only (for Aufgabe 2c)
CREATE OR REPLACE VIEW vw_First_Visit_Only AS
SELECT *
FROM Fact_Untersuchung
WHERE IsFirstVisit = TRUE;

-- View: Patient Examination Summary (First Visit)
CREATE OR REPLACE VIEW vw_Patient_Examination_Summary AS
SELECT 
    dp.PatientKey,
    dp.Nachname,
    dp.Vorname,
    dp.AgeGroup,
    dp.Geschlecht,
    dp.Versicherung,
    pr.PraxisName,
    pr.PraxisType,
    dic.ICD_Code,
    dic.Code_Titel,
    dic.GruppeCode,
    dic.GruppeTitel,
    dic.KapitelNr,
    dic.KapitelTitel,
    fu.Tensio,
    fu.Refraktion,
    fu.Visus,
    fu.Untersuchungsdatum
FROM Dim_Patient dp
JOIN Fact_Untersuchung fu ON dp.PatientKey = fu.PatientKey
JOIN Dim_Date dd ON fu.DateKey = dd.DateKey
JOIN Dim_Praxis pr ON fu.PraxisKey = pr.PraxisKey
LEFT JOIN Dim_ICD_Code dic ON fu.ICDKey = dic.ICDKey
WHERE dp.IsCurrent = TRUE
  AND fu.IsFirstVisit = TRUE;

-- View: Average Measurements by Age Group (for Aufgabe 2)
CREATE OR REPLACE VIEW vw_Measurements_By_AgeGroup AS
SELECT 
    dp.AgeGroup,
    dic.ICD_Code,
    dic.Code_Titel,
    dic.KapitelNr,
    dic.KapitelTitel,
    pr.PraxisType,
    COUNT(DISTINCT dp.PatientKey) AS PatientCount,
    AVG(fu.Tensio) AS AvgTensio,
    AVG(fu.Refraktion) AS AvgRefraktion,
    AVG(fu.Visus) AS AvgVisus
FROM Dim_Patient dp
JOIN Fact_Untersuchung fu ON dp.PatientKey = fu.PatientKey
JOIN Dim_Praxis pr ON fu.PraxisKey = pr.PraxisKey
LEFT JOIN Dim_ICD_Code dic ON fu.ICDKey = dic.ICDKey
WHERE dp.IsCurrent = TRUE
  AND fu.IsFirstVisit = TRUE
GROUP BY dp.AgeGroup, dic.ICD_Code, dic.Code_Titel, 
         dic.KapitelNr, dic.KapitelTitel, pr.PraxisType
ORDER BY dp.AgeGroup;

-- View: Anamnese-Diagnose Correlation (for Aufgabe 3a)
CREATE OR REPLACE VIEW vw_Anamnese_Diagnose_Correlation AS
SELECT 
    anamnese.ICD_Code AS Anamnese_ICD,
    anamnese.Code_Titel AS Anamnese_Titel,
    diagnose.ICD_Code AS Diagnose_ICD,
    diagnose.Code_Titel AS Diagnose_Titel,
    COUNT(DISTINCT dp.PatientKey) AS PatientCount
FROM Dim_Patient dp
JOIN Fact_Patient_Anamnese fpa ON dp.PatientKey = fpa.PatientKey
JOIN Dim_ICD_Code anamnese ON fpa.ICDKey = anamnese.ICDKey
JOIN Fact_Untersuchung fu ON dp.PatientKey = fu.PatientKey
JOIN Dim_ICD_Code diagnose ON fu.ICDKey = diagnose.ICDKey
WHERE dp.IsCurrent = TRUE
  AND anamnese.IsCurrent = TRUE
  AND diagnose.IsCurrent = TRUE
GROUP BY anamnese.ICD_Code, anamnese.Code_Titel,
         diagnose.ICD_Code, diagnose.Code_Titel
ORDER BY PatientCount DESC;

-- View: Versicherung Distribution (for Aufgabe 3e)
CREATE OR REPLACE VIEW vw_Versicherung_Distribution AS
SELECT 
    pr.PraxisType,
    dp.Versicherung,
    COUNT(DISTINCT dp.PatientKey) AS PatientCount,
    ROUND(100.0 * COUNT(DISTINCT dp.PatientKey) / 
          SUM(COUNT(DISTINCT dp.PatientKey)) OVER (PARTITION BY pr.PraxisType), 2) AS Percentage
FROM Dim_Patient dp
JOIN Fact_Untersuchung fu ON dp.PatientKey = fu.PatientKey
JOIN Dim_Praxis pr ON fu.PraxisKey = pr.PraxisKey
WHERE dp.IsCurrent = TRUE
GROUP BY pr.PraxisType, dp.Versicherung
ORDER BY pr.PraxisType, PatientCount DESC;
