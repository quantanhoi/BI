CREATE TABLE mydb.icd_codes (
  id INT AUTO_INCREMENT PRIMARY KEY,
  code VARCHAR(10),              -- A00, A00.0, A00.1, ...
  gruppen_code VARCHAR(10),
  diagnose TEXT

);



LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/ICD_Codes.csv'
INTO TABLE mydb.icd_codes
FIELDS TERMINATED BY ';'
LINES TERMINATED BY '\r\n'
(@c1,@c2,@c3,@c4,@c5,@c6,@c7,@c8,@c9,@c10,@c11,@c12,@c13,@c14,@c15,@c16,@c17,@c18,@c19,@c20,@c21,@c22,@c23,@c24,@c27,@c26,@c27,@c28)
SET
  code = @c5,
  gruppen_code = @c6,
  diagnose = @c9;

  
  CREATE TABLE mydb.praxis (
  praxis_id CHAR(36) PRIMARY KEY,
  praxis_name VARCHAR(100),
  ort Varchar(100)
);



ALTER TABLE mydb.stammdaten
  ADD praxis_id CHAR(36),
  ADD externe_patientennummer VARCHAR(50);
  
  ALTER TABLE mydb.stammdaten
  
  ADD Patient_ID CHAR(36);

ALTER TABLE mydb.praxis
CHANGE COLUMN ort kreis VARCHAR(100),
ADD COLUMN bezirk VARCHAR(100),
ADD COLUMN bundesland VARCHAR(100);

RENAME TABLE mydb.praxis TO mydb.einrichtung;
ALTER TABLE mydb.einrichtung
ADD COLUMN typ ENUM('Praxis','Klinik');
INSERT INTO mydb.einrichtung (praxis_id, praxis_name, kreis, bezirk, bundesland, typ) VALUES
(UUID(), 'Praxis Coesfeld',       'Coesfeld',       'Münster',  'NRW', 'Praxis'),
(UUID(), 'Praxis Hamm',           'Hamm',           'Arnsberg', 'NRW', 'Praxis'),
(UUID(), 'Praxis Münster',        'Münster',        'Münster',  'NRW', 'Praxis'),
(UUID(), 'Praxis Telgte',         'Warendorf',      'Münster',  'NRW', 'Praxis'),
(UUID(), 'Praxis Unna',           'Unna',           'Arnsberg', 'NRW', 'Praxis'),
(UUID(), 'Praxis Warendorf',      'Warendorf',      'Münster',  'NRW', 'Praxis'),
(UUID(), 'Praxis Neunkirchen',    'Neunkirchen',    'Saarland', 'Saarland', 'Praxis'),
(UUID(), 'Praxis Pirmasens',      'Pirmasens',      'Pfalz',    'Rheinland-Pfalz', 'Praxis'),
(UUID(), 'Praxis St. Wendel',     'St. Wendel',     'Saarland', 'Saarland', 'Praxis'),
(UUID(), 'Praxis Zweibrücken',    'Zweibrücken',    'Pfalz',    'Rheinland-Pfalz', 'Praxis');
INSERT INTO mydb.einrichtung(praxis_id, praxis_name, kreis, bezirk, bundesland, typ) VALUES
(UUID(), 'Uniklinik Münster',   'Münster', 'Münster', 'NRW', 'Klinik'),
(UUID(), 'Uniklinik Saarland',  'Homburg', 'Saarpfalz', 'Saarland', 'Klinik');

ALter Table mydb.anamnesen
ADD praxis_id CHAR(36),
ADD externe_patientennummer VARCHAR(50),
 ADD Patient_ID CHAR(36);
 
 ALter Table mydb.messwerte
ADD praxis_id CHAR(36),
ADD externe_patientennummer VARCHAR(50),
 ADD Patient_ID CHAR(36);
 
 ALTER TABLE mydb.stammdaten
ADD INDEX idx_patient_id (Patient_ID);


ALTER TABLE mydb.stammdaten
ADD INDEX idx_patient_id (Patient_ID);

ALTER TABLE mydb.anamnesen
ADD INDEX idx_anamnesen_patient_id (Patient_ID);

Delete From mydb.stammdaten;

ALTER TABLE mydb.icd_codes AUTO_INCREMENT = 1;

Update mydb.einrichtung
 Set kreis="Telgte"
where praxis_id="f6515ab4-c49c-11f0-b8bd-089798e55be8";



ALTER TABLE mydb.messwerte
MODIFY visus Varchar(50);

#2) auufgabe
SHOW DATABASES;








  
  








