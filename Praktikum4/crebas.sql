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
    IsWeekend BOOLEAN NOT NULL,
    FiscalYear INT,
    FiscalQuarter INT
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
    Region VARCHAR(100),
    LoadDate TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- -----------------------------------------------------------------------------
-- Dim_Patient - Patient Dimension (using Master Patient data when available)
-- -----------------------------------------------------------------------------
CREATE TABLE Dim_Patient (
    PatientKey SERIAL PRIMARY KEY,
    HK_Patient CHAR(32) NOT NULL,           -- Link to Data Vault
    HK_Patient_Master CHAR(32),             -- Link to Master Patient if exists
    MasterPatientID VARCHAR(50),            -- Unified patient ID
    PatientID VARCHAR(50) NOT NULL,         -- Original patient ID
    PatientSource VARCHAR(50) NOT NULL,     -- Original source
    Nachname VARCHAR(100),
    Vorname VARCHAR(100),
    Geburtsdatum DATE,
    Versicherung VARCHAR(50),
    Geschlecht VARCHAR(10),
    AgeGroup VARCHAR(20),                   -- Derived: '0-17', '18-30', '31-45', '46-60', '61-75', '76+'
    IsMasterRecord BOOLEAN DEFAULT FALSE,   -- True if has unified master record
    EffectiveDate DATE NOT NULL,
    ExpirationDate DATE DEFAULT '9999-12-31',
    IsCurrent BOOLEAN DEFAULT TRUE,         -- SCD Type 2 current flag
    LoadDate TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Index for lookups
CREATE INDEX idx_dim_patient_hk ON Dim_Patient(HK_Patient);
CREATE INDEX idx_dim_patient_master ON Dim_Patient(HK_Patient_Master);
CREATE INDEX idx_dim_patient_current ON Dim_Patient(IsCurrent);

-- -----------------------------------------------------------------------------
-- Dim_ICD_Code - ICD Code Dimension with hierarchy (denormalized)
-- -----------------------------------------------------------------------------
CREATE TABLE Dim_ICD_Code (
    ICDKey SERIAL PRIMARY KEY,
    HK_ICD_Code CHAR(32) NOT NULL,          -- Link to Data Vault
    ICD_Code VARCHAR(20) NOT NULL,
    Code_Titel VARCHAR(255),
    GruppeCode VARCHAR(20),
    GruppeTitel VARCHAR(255),
    Bis_Code VARCHAR(20),
    KapitelNr VARCHAR(10),
    KapitelTitel VARCHAR(255),
    EffectiveDate DATE NOT NULL,
    ExpirationDate DATE DEFAULT '9999-12-31',
    IsCurrent BOOLEAN DEFAULT TRUE,
    LoadDate TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Index for lookups
CREATE INDEX idx_dim_icd_code_hk ON Dim_ICD_Code(HK_ICD_Code);
CREATE INDEX idx_dim_icd_code_current ON Dim_ICD_Code(IsCurrent);
CREATE INDEX idx_dim_icd_kapitel ON Dim_ICD_Code(KapitelNr);
CREATE INDEX idx_dim_icd_gruppe ON Dim_ICD_Code(GruppeCode);

-- =============================================================================
-- Fact Tables
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Fact_Untersuchung - Examination Facts with measurements
-- -----------------------------------------------------------------------------
CREATE TABLE Fact_Untersuchung (
    UntersuchungFactKey SERIAL PRIMARY KEY,
    HK_Untersuchung CHAR(32) NOT NULL,      -- Link to Data Vault
    PatientKey INT NOT NULL REFERENCES Dim_Patient(PatientKey),
    ICDKey INT REFERENCES Dim_ICD_Code(ICDKey),  -- May be NULL if no diagnosis
    DateKey INT NOT NULL REFERENCES Dim_Date(DateKey),
    PraxisKey INT NOT NULL REFERENCES Dim_Praxis(PraxisKey),
    
    -- Measures from Sat_Untersuchung and Sat_Messwerte
    Untersuchungsart VARCHAR(100),
    Untersucher VARCHAR(100),
    Tensio NUMERIC(10, 2),
    Refraktion NUMERIC(10, 2),
    Visus NUMERIC(10, 2),
    
    -- Derived measures
    HasMeasurements BOOLEAN DEFAULT FALSE,
    DiagnoseCount INT DEFAULT 0,
    
    LoadDate TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for common query patterns
CREATE INDEX idx_fact_untersuchung_patient ON Fact_Untersuchung(PatientKey);
CREATE INDEX idx_fact_untersuchung_date ON Fact_Untersuchung(DateKey);
CREATE INDEX idx_fact_untersuchung_praxis ON Fact_Untersuchung(PraxisKey);
CREATE INDEX idx_fact_untersuchung_icd ON Fact_Untersuchung(ICDKey);
CREATE INDEX idx_fact_untersuchung_hk ON Fact_Untersuchung(HK_Untersuchung);

-- -----------------------------------------------------------------------------
-- Fact_Patient_Anamnese - Patient Medical History Facts
-- -----------------------------------------------------------------------------
CREATE TABLE Fact_Patient_Anamnese (
    AnamneseFactKey SERIAL PRIMARY KEY,
    PatientKey INT NOT NULL REFERENCES Dim_Patient(PatientKey),
    ICDKey INT NOT NULL REFERENCES Dim_ICD_Code(ICDKey),
    PraxisKey INT NOT NULL REFERENCES Dim_Praxis(PraxisKey),
    
    -- Link back to Data Vault
    HK_Patient CHAR(32) NOT NULL,
    HK_ICD_Code CHAR(32) NOT NULL,
    
    LoadDate TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Indexes
CREATE INDEX idx_fact_anamnese_patient ON Fact_Patient_Anamnese(PatientKey);
CREATE INDEX idx_fact_anamnese_icd ON Fact_Patient_Anamnese(ICDKey);
CREATE INDEX idx_fact_anamnese_praxis ON Fact_Patient_Anamnese(PraxisKey);

-- =============================================================================
-- Views for Analysis
-- =============================================================================

-- View: Patient Examination Summary
CREATE OR REPLACE VIEW vw_Patient_Examination_Summary AS
SELECT 
    dp.PatientKey,
    dp.Nachname,
    dp.Vorname,
    dp.AgeGroup,
    dp.Geschlecht,
    pr.PraxisName,
    pr.City,
    COUNT(DISTINCT fu.UntersuchungFactKey) AS TotalExaminations,
    COUNT(DISTINCT fu.ICDKey) AS UniqueDiagnoses,
    AVG(fu.Tensio) AS AvgTensio,
    AVG(fu.Refraktion) AS AvgRefraktion,
    AVG(fu.Visus) AS AvgVisus,
    MIN(dd.FullDate) AS FirstExamination,
    MAX(dd.FullDate) AS LastExamination
FROM Dim_Patient dp
JOIN Fact_Untersuchung fu ON dp.PatientKey = fu.PatientKey
JOIN Dim_Date dd ON fu.DateKey = dd.DateKey
JOIN Dim_Praxis pr ON fu.PraxisKey = pr.PraxisKey
WHERE dp.IsCurrent = TRUE
GROUP BY dp.PatientKey, dp.Nachname, dp.Vorname, dp.AgeGroup, dp.Geschlecht, 
         pr.PraxisName, pr.City;

-- View: ICD Code Analysis
CREATE OR REPLACE VIEW vw_ICD_Analysis AS
SELECT 
    dic.KapitelNr,
    dic.KapitelTitel,
    dic.GruppeCode,
    dic.GruppeTitel,
    dic.ICD_Code,
    dic.Code_Titel,
    COUNT(DISTINCT fu.PatientKey) AS PatientCount,
    COUNT(fu.UntersuchungFactKey) AS DiagnosisCount,
    pr.PraxisType,
    pr.City
FROM Dim_ICD_Code dic
JOIN Fact_Untersuchung fu ON dic.ICDKey = fu.ICDKey
JOIN Dim_Praxis pr ON fu.PraxisKey = pr.PraxisKey
WHERE dic.IsCurrent = TRUE
GROUP BY dic.KapitelNr, dic.KapitelTitel, dic.GruppeCode, dic.GruppeTitel,
         dic.ICD_Code, dic.Code_Titel, pr.PraxisType, pr.City;

-- View: Monthly Examination Trends
CREATE OR REPLACE VIEW vw_Monthly_Trends AS
SELECT 
    dd.Year,
    dd.MonthNumber,
    dd.MonthName,
    pr.PraxisName,
    pr.PraxisType,
    COUNT(DISTINCT fu.UntersuchungFactKey) AS ExaminationCount,
    COUNT(DISTINCT fu.PatientKey) AS UniquePatients,
    AVG(fu.DiagnoseCount) AS AvgDiagnosesPerExam
FROM Fact_Untersuchung fu
JOIN Dim_Date dd ON fu.DateKey = dd.DateKey
JOIN Dim_Praxis pr ON fu.PraxisKey = pr.PraxisKey
GROUP BY dd.Year, dd.MonthNumber, dd.MonthName, pr.PraxisName, pr.PraxisType
ORDER BY dd.Year, dd.MonthNumber;
