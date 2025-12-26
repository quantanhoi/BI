DROP TABLE IF EXISTS Sat_Messwerte CASCADE;
DROP TABLE IF EXISTS Sat_Patient_Stammdaten CASCADE;
DROP TABLE IF EXISTS Sat_ICD_Code CASCADE;
DROP TABLE IF EXISTS Sat_ICD_Gruppe CASCADE;
DROP TABLE IF EXISTS Sat_ICD_Kapitel CASCADE;
DROP TABLE IF EXISTS Link_Patient_Diagnose CASCADE;
DROP TABLE IF EXISTS Link_Patient_Anamnese CASCADE;
DROP TABLE IF EXISTS Link_Gruppe_Code CASCADE;
DROP TABLE IF EXISTS Link_Kapitel_Gruppe CASCADE;
DROP TABLE IF EXISTS Hub_Patient CASCADE;
DROP TABLE IF EXISTS Hub_ICD_Code CASCADE;
DROP TABLE IF EXISTS Hub_ICD_Gruppe CASCADE;
DROP TABLE IF EXISTS Hub_ICD_Kapitel CASCADE;

CREATE TABLE Hub_ICD_Kapitel (
    HK_ICD_Kapitel CHAR(32) PRIMARY KEY,
    KapitelNr VARCHAR(10) NOT NULL UNIQUE,
    LoadDate TIMESTAMP NOT NULL,
    RecordSource VARCHAR(100) NOT NULL
);

CREATE TABLE Hub_ICD_Gruppe (
    HK_ICD_Gruppe CHAR(32) PRIMARY KEY,
    GruppeCode VARCHAR(20) NOT NULL UNIQUE, -- e.g., 'A00'
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
    PatientSource VARCHAR(50) NOT NULL, -- e.g., 'praxis_coesfeld', 'uniklinik_muenster'
    LoadDate TIMESTAMP NOT NULL,
    RecordSource VARCHAR(100) NOT NULL,
    CONSTRAINT UQ_Hub_Patient UNIQUE (PatientID, PatientSource)
);

-- =============================================================================
-- Links
-- =============================================================================

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

CREATE TABLE Link_Patient_Anamnese (
    HK_Link_Patient_Anamnese CHAR(32) PRIMARY KEY,
    HK_Patient CHAR(32) NOT NULL,
    HK_ICD_Code CHAR(32) NOT NULL,
    LoadDate TIMESTAMP NOT NULL,
    RecordSource VARCHAR(100) NOT NULL,
    FOREIGN KEY (HK_Patient) REFERENCES Hub_Patient(HK_Patient),
    FOREIGN KEY (HK_ICD_Code) REFERENCES Hub_ICD_Code(HK_ICD_Code)
);

CREATE TABLE Link_Patient_Diagnose (
    HK_Link_Patient_Diagnose CHAR(32) PRIMARY KEY,
    HK_Patient CHAR(32) NOT NULL,
    HK_ICD_Code CHAR(32) NOT NULL,
    LoadDate TIMESTAMP NOT NULL,
    RecordSource VARCHAR(100) NOT NULL,
    FOREIGN KEY (HK_Patient) REFERENCES Hub_Patient(HK_Patient),
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
    -- Add other attributes from ICD_Codes.csv as needed
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

-- Link Satellite on Link_Patient_Diagnose
CREATE TABLE Sat_Messwerte (
    HK_Link_Patient_Diagnose CHAR(32) NOT NULL,
    LoadDate TIMESTAMP NOT NULL,
    HashDiff CHAR(32) NOT NULL,
    RecordSource VARCHAR(100) NOT NULL,
    Tensio NUMERIC(10, 2),
    Refraktion NUMERIC(10, 2),
    Visus NUMERIC(10, 2),
    Untersuchungsdatum DATE,
    PRIMARY KEY (HK_Link_Patient_Diagnose, LoadDate),
    FOREIGN KEY (HK_Link_Patient_Diagnose) REFERENCES Link_Patient_Diagnose(HK_Link_Patient_Diagnose)
);
