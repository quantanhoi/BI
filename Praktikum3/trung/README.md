# Data Vault — Quick Query Map

| Question | How |
|---|---|
| List all diagnoses for patient X | Join Hub_Patient → Link_Patient_Untersuchung → Link_Untersuchung_Diagnose → Hub_ICD_Code |
| How many examinations did patient X have? | Count distinct Hub_Untersuchung via Link_Patient_Untersuchung |
| What were patient X's Visus measurements over time? | Join Hub_Patient → Link_Patient_Untersuchung → Hub_Untersuchung → Sat_Messwerte + Sat_Untersuchung (for date) |
| Which diagnoses were made on examination Y? | Join Hub_Untersuchung → Link_Untersuchung_Diagnose → Hub_ICD_Code |
| What's patient X's medical history (Anamnese)? | Join Hub_Patient → Link_Patient_Anamnese → Hub_ICD_Code |



## Known Problem with first edition data vault model
![current pdm](docs/pdm_ICD_datavault.png)

With this current physical data model, the problem with this model is that there is no unified patient key, and data is still separated by origin.

Die Daten sollen im Data Warehouse mit einem einheitlichen Patientenschlüssel zusammengelegt und nicht mehr nach Herkunft getrennt werden => Data should be merged with an unified patient key and no longer separated by origin

Current schema: 
```
CONSTRAINT UQ_Hub_Patient UNIQUE (PatientID, PatientSource)
```

This mean:
- Patient "12345" from praxis_coesfeld => Hub entry 1
- Patient "12345" from uniklinik_saarland => Hub entry 2

Even if this is the same person, they remain separated in this data vault schema

### Proposed solution?\

**Unified Patient**: If the same person visit different kliniks, how do we know that it's the same person?
- This is the fundamental problem, normally this would be easier if we have
1. Versicherungssnummer
2. Echte ID Nummer
3. Adresse oder Postanschrift
Currently we have none of these here, What is the chance of 2 People having the same name and same birthday?

## Knime Workflow
### Hub and Sat
Currently for the hub and satelitte we are using this workflow for ICD Gruppe, Codes and Kapitel
To keep this simple, the table codes will only save the code and code description, other value with unknown description will not be saved

![hub and sat workflow](docs/hubsat_workflow.png)


### Link

After inserting Hub and Sat, the last thing we need to insert is Link tables, which is as followed:

![link workflow](docs/link_kap_gruppe.png)


### Patient
Now that we are done with ICD data, the next thing would be data from patients from different Praxis and Klinik

We could reuse the same component for filtering row, db connect and reader and joiner for writing into database

To create a loop for each type of data from different Praxis/Klinik, we use List Files/Folder and Table Row to Variable Loop Start + Loop End Nodes to create a loop

Basically it will loop through all Stammdaten files (from all Praxis/Klinik) in the folder and execute the ETL pipeline

![hub and sat Patient Workflow](docs/knime_patient_hub_sat.png)


The workflow is basically the same across hub patient, hub untersuchung, sat messwert, etc... 

The differences are the python scripts and what Row Filter nodes filter out

If you want to see the full data pipeline in knime, you can load the MLOPS.knwf into KNIME