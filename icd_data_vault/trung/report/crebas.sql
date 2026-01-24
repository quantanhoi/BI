DROP TABLE IF EXISTS Sat_Messwerte CASCADE;
DROP TABLE IF EXISTS Sat_Untersuchung CASCADE;
DROP TABLE IF EXISTS Sat_Patient_Master_Stammdaten CASCADE;
DROP TABLE IF EXISTS Sat_Patient_Stammdaten CASCADE;
DROP TABLE IF EXISTS Sat_ICD_Code CASCADE;
DROP TABLE IF EXISTS Sat_ICD_Gruppe CASCADE;
DROP TABLE IF EXISTS Sat_ICD_Kapitel CASCADE;
DROP TABLE IF EXISTS Link_Untersuchung_Diagnose CASCADE;
DROP TABLE IF EXISTS Link_Patient_Untersuchung CASCADE;
DROP TABLE IF EXISTS Link_Patient_Master CASCADE;
DROP TABLE IF EXISTS Link_Patient_Anamnese CASCADE;
DROP TABLE IF EXISTS Link_Gruppe_Code CASCADE;
DROP TABLE IF EXISTS Link_Kapitel_Gruppe CASCADE;
DROP TABLE IF EXISTS Hub_Untersuchung CASCADE;
DROP TABLE IF EXISTS Hub_Patient_Master CASCADE;
DROP TABLE IF EXISTS Hub_Patient CASCADE;
DROP TABLE IF EXISTS Hub_ICD_Code CASCADE;
DROP TABLE IF EXISTS Hub_ICD_Gruppe CASCADE;
DROP TABLE IF EXISTS Hub_ICD_Kapitel CASCADE;

-- =============================================================================
-- Hubs
-- =============================================================================

CREATE TABLE Hub_ICD_Kapitel (
    HK_ICD_Kapitel CHAR(32) PRIMARY KEY,
    KapitelNr VARCHAR(10) NOT NULL UNIQUE,
    LoadDate TIMESTAMP NOT NULL,
    RecordSource VARCHAR(100) NOT NULL
);

CREATE TABLE Hub_ICD_Gruppe (
    HK_ICD_Gruppe CHAR(32) PRIMARY KEY,
    GruppeCode VARCHAR(20) NOT NULL UNIQUE,
    LoadDate TIMESTAMP NOT NULL,
    RecordSource VARCHAR(100) NOT NULL
);

CREATE TABLE Hub_ICD_Code (
    HK_ICD_Code CHAR(32) PRIMARY KEY,
    ICD_Code VARCHAR(20) NOT NULL UNIQUE,
    LoadDate TIMESTAMP NOT NULL,
    RecordSource VARCHAR(100) NOT NULL
);

CREATE TABLE Hub_Patient (
    HK_Patient CHAR(32) PRIMARY KEY,
    PatientID VARCHAR(50) NOT NULL,
    PatientSource VARCHAR(50) NOT NULL,
    LoadDate TIMESTAMP NOT NULL,
    RecordSource VARCHAR(100) NOT NULL,
    CONSTRAINT UQ_Hub_Patient UNIQUE (PatientID, PatientSource)
);

-- Hub for unified patient records (Master Patient Index)
CREATE TABLE Hub_Patient_Master (
    HK_Patient_Master CHAR(32) PRIMARY KEY,
    MasterPatientID VARCHAR(50) NOT NULL UNIQUE,
    LoadDate TIMESTAMP NOT NULL,
    RecordSource VARCHAR(100) NOT NULL
);

-- NEW: Hub for examinations/visits
CREATE TABLE Hub_Untersuchung (
    HK_Untersuchung CHAR(32) PRIMARY KEY,
    UntersuchungsID VARCHAR(50) NOT NULL,
    UntersuchungsSource VARCHAR(50) NOT NULL,
    LoadDate TIMESTAMP NOT NULL,
    RecordSource VARCHAR(100) NOT NULL,
    CONSTRAINT UQ_Hub_Untersuchung UNIQUE (UntersuchungsID, UntersuchungsSource)
);

-- =============================================================================
-- Links
-- =============================================================================

-- ICD Hierarchy Links (unchanged)
CREATE TABLE Link_Kapitel_Gruppe (
    HK_Link_Kapitel_Gruppe CHAR(32) PRIMARY KEY,
    HK_ICD_Kapitel CHAR(32) NOT NULL,
    HK_ICD_Gruppe CHAR(32) NOT NULL,
    LoadDate TIMESTAMP NOT NULL,
    RecordSource VARCHAR(100) NOT NULL,
    FOREIGN KEY (HK_ICD_Kapitel) REFERENCES Hub_ICD_Kapitel(HK_ICD_Kapitel),
    FOREIGN KEY (HK_ICD_Gruppe) REFERENCES Hub_ICD_Gruppe(HK_ICD_Gruppe)
);

CREATE TABLE Link_Gruppe_Code (
    HK_Link_Gruppe_Code CHAR(32) PRIMARY KEY,
    HK_ICD_Gruppe CHAR(32) NOT NULL,
    HK_ICD_Code CHAR(32) NOT NULL,
    LoadDate TIMESTAMP NOT NULL,
    RecordSource VARCHAR(100) NOT NULL,
    FOREIGN KEY (HK_ICD_Gruppe) REFERENCES Hub_ICD_Gruppe(HK_ICD_Gruppe),
    FOREIGN KEY (HK_ICD_Code) REFERENCES Hub_ICD_Code(HK_ICD_Code)
);

-- Patient's historical conditions (medical history, not from current exam)
CREATE TABLE Link_Patient_Anamnese (
    HK_Link_Patient_Anamnese CHAR(32) PRIMARY KEY,
    HK_Patient CHAR(32) NOT NULL,
    HK_ICD_Code CHAR(32) NOT NULL,
    LoadDate TIMESTAMP NOT NULL,
    RecordSource VARCHAR(100) NOT NULL,
    FOREIGN KEY (HK_Patient) REFERENCES Hub_Patient(HK_Patient),
    FOREIGN KEY (HK_ICD_Code) REFERENCES Hub_ICD_Code(HK_ICD_Code)
);

-- Links raw patients to their unified master record
CREATE TABLE Link_Patient_Master (
    HK_Link_Patient_Master CHAR(32) PRIMARY KEY,
    HK_Patient CHAR(32) NOT NULL,
    HK_Patient_Master CHAR(32) NOT NULL,
    MatchMethod VARCHAR(50),
    LevenshteinScore_Nachname INT,
    LevenshteinScore_Vorname INT,
    LoadDate TIMESTAMP NOT NULL,
    RecordSource VARCHAR(100) NOT NULL,
    FOREIGN KEY (HK_Patient) REFERENCES Hub_Patient(HK_Patient),
    FOREIGN KEY (HK_Patient_Master) REFERENCES Hub_Patient_Master(HK_Patient_Master)
);

-- NEW: Links patient to examination (who visited when)
CREATE TABLE Link_Patient_Untersuchung (
    HK_Link_Patient_Untersuchung CHAR(32) PRIMARY KEY,
    HK_Patient CHAR(32) NOT NULL,
    HK_Untersuchung CHAR(32) NOT NULL,
    LoadDate TIMESTAMP NOT NULL,
    RecordSource VARCHAR(100) NOT NULL,
    FOREIGN KEY (HK_Patient) REFERENCES Hub_Patient(HK_Patient),
    FOREIGN KEY (HK_Untersuchung) REFERENCES Hub_Untersuchung(HK_Untersuchung)
);

-- NEW: Links examination to diagnosis (what was diagnosed in this exam)
CREATE TABLE Link_Untersuchung_Diagnose (
    HK_Link_Untersuchung_Diagnose CHAR(32) PRIMARY KEY,
    HK_Untersuchung CHAR(32) NOT NULL,
    HK_ICD_Code CHAR(32) NOT NULL,
    LoadDate TIMESTAMP NOT NULL,
    RecordSource VARCHAR(100) NOT NULL,
    FOREIGN KEY (HK_Untersuchung) REFERENCES Hub_Untersuchung(HK_Untersuchung),
    FOREIGN KEY (HK_ICD_Code) REFERENCES Hub_ICD_Code(HK_ICD_Code)
);

-- =============================================================================
-- Satellites
-- =============================================================================

CREATE TABLE Sat_ICD_Kapitel (
    HK_ICD_Kapitel CHAR(32) NOT NULL,
    LoadDate TIMESTAMP NOT NULL,
    HashDiff CHAR(32) NOT NULL,
    RecordSource VARCHAR(100) NOT NULL,
    KapitelTitel VARCHAR(255),
    PRIMARY KEY (HK_ICD_Kapitel, LoadDate),
    FOREIGN KEY (HK_ICD_Kapitel) REFERENCES Hub_ICD_Kapitel(HK_ICD_Kapitel)
);

CREATE TABLE Sat_ICD_Gruppe (
    HK_ICD_Gruppe CHAR(32) NOT NULL,
    LoadDate TIMESTAMP NOT NULL,
    HashDiff CHAR(32) NOT NULL,
    RecordSource VARCHAR(100) NOT NULL,
    GruppeTitel VARCHAR(255),
    Bis_Code VARCHAR(20),
    PRIMARY KEY (HK_ICD_Gruppe, LoadDate),
    FOREIGN KEY (HK_ICD_Gruppe) REFERENCES Hub_ICD_Gruppe(HK_ICD_Gruppe)
);

CREATE TABLE Sat_ICD_Code (
    HK_ICD_Code CHAR(32) NOT NULL,
    LoadDate TIMESTAMP NOT NULL,
    HashDiff CHAR(32) NOT NULL,
    RecordSource VARCHAR(100) NOT NULL,
    Code_Titel VARCHAR(255),
    PRIMARY KEY (HK_ICD_Code, LoadDate),
    FOREIGN KEY (HK_ICD_Code) REFERENCES Hub_ICD_Code(HK_ICD_Code)
);

CREATE TABLE Sat_Patient_Stammdaten (
    HK_Patient CHAR(32) NOT NULL,
    LoadDate TIMESTAMP NOT NULL,
    HashDiff CHAR(32) NOT NULL,
    RecordSource VARCHAR(100) NOT NULL,
    Nachname VARCHAR(100),
    Vorname VARCHAR(100),
    Geburtsdatum DATE,
    Versicherung VARCHAR(50),
    Geschlecht VARCHAR(10),
    PRIMARY KEY (HK_Patient, LoadDate),
    FOREIGN KEY (HK_Patient) REFERENCES Hub_Patient(HK_Patient)
);

-- Satellite for unified master patient data (golden record)
CREATE TABLE Sat_Patient_Master_Stammdaten (
    HK_Patient_Master CHAR(32) NOT NULL,
    LoadDate TIMESTAMP NOT NULL,
    HashDiff CHAR(32) NOT NULL,
    RecordSource VARCHAR(100) NOT NULL,
    Nachname VARCHAR(100),
    Vorname VARCHAR(100),
    Geburtsdatum DATE,
    PRIMARY KEY (HK_Patient_Master, LoadDate),
    FOREIGN KEY (HK_Patient_Master) REFERENCES Hub_Patient_Master(HK_Patient_Master)
);

-- NEW: Descriptive attributes for examinations
CREATE TABLE Sat_Untersuchung (
    HK_Untersuchung CHAR(32) NOT NULL,
    LoadDate TIMESTAMP NOT NULL,
    HashDiff CHAR(32) NOT NULL,
    RecordSource VARCHAR(100) NOT NULL,
    Untersuchungsdatum DATE NOT NULL,
    Untersuchungsart VARCHAR(100),
    Untersucher VARCHAR(100),
    PRIMARY KEY (HK_Untersuchung, LoadDate),
    FOREIGN KEY (HK_Untersuchung) REFERENCES Hub_Untersuchung(HK_Untersuchung)
);

-- Measurements now attached to examination (not to patient-diagnosis)
CREATE TABLE Sat_Messwerte (
    HK_Untersuchung CHAR(32) NOT NULL,
    LoadDate TIMESTAMP NOT NULL,
    HashDiff CHAR(32) NOT NULL,
    RecordSource VARCHAR(100) NOT NULL,
    Tensio NUMERIC(10, 2),
    Refraktion NUMERIC(10, 2),
    Visus NUMERIC(10, 2),
    PRIMARY KEY (HK_Untersuchung, LoadDate),
    FOREIGN KEY (HK_Untersuchung) REFERENCES Hub_Untersuchung(HK_Untersuchung)
);

-- =============================================================================
-- Indexes for performance
-- =============================================================================

-- Link foreign keys
CREATE INDEX idx_link_kapitel_gruppe_kapitel ON Link_Kapitel_Gruppe(HK_ICD_Kapitel);
CREATE INDEX idx_link_kapitel_gruppe_gruppe ON Link_Kapitel_Gruppe(HK_ICD_Gruppe);

CREATE INDEX idx_link_gruppe_code_gruppe ON Link_Gruppe_Code(HK_ICD_Gruppe);
CREATE INDEX idx_link_gruppe_code_code ON Link_Gruppe_Code(HK_ICD_Code);

CREATE INDEX idx_link_patient_anamnese_patient ON Link_Patient_Anamnese(HK_Patient);
CREATE INDEX idx_link_patient_anamnese_code ON Link_Patient_Anamnese(HK_ICD_Code);

CREATE INDEX idx_link_patient_master_patient ON Link_Patient_Master(HK_Patient);
CREATE INDEX idx_link_patient_master_master ON Link_Patient_Master(HK_Patient_Master);

CREATE INDEX idx_link_patient_untersuchung_patient ON Link_Patient_Untersuchung(HK_Patient);
CREATE INDEX idx_link_patient_untersuchung_untersuchung ON Link_Patient_Untersuchung(HK_Untersuchung);

CREATE INDEX idx_link_untersuchung_diagnose_untersuchung ON Link_Untersuchung_Diagnose(HK_Untersuchung);
CREATE INDEX idx_link_untersuchung_diagnose_code ON Link_Untersuchung_Diagnose(HK_ICD_Code);

-- Satellite foreign keys
CREATE INDEX idx_sat_icd_kapitel_hk ON Sat_ICD_Kapitel(HK_ICD_Kapitel);
CREATE INDEX idx_sat_icd_gruppe_hk ON Sat_ICD_Gruppe(HK_ICD_Gruppe);
CREATE INDEX idx_sat_icd_code_hk ON Sat_ICD_Code(HK_ICD_Code);
CREATE INDEX idx_sat_patient_stammdaten_hk ON Sat_Patient_Stammdaten(HK_Patient);
CREATE INDEX idx_sat_patient_master_stammdaten_hk ON Sat_Patient_Master_Stammdaten(HK_Patient_Master);
CREATE INDEX idx_sat_untersuchung_hk ON Sat_Untersuchung(HK_Untersuchung);
CREATE INDEX idx_sat_messwerte_hk ON Sat_Messwerte(HK_Untersuchung);
