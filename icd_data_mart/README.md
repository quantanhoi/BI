# Praktikum 4 - Gruppe 10

```
DiagnosisCount = 
CALCULATE(
    COUNT('public fact_untersuchung'[UntersuchungFactKey]),
    NOT(ISBLANK('public fact_untersuchung'[ICDKey])),
    'public dim_icd_code'[IsCurrent] = TRUE
)
```


```
AffectedPatients = 
CALCULATE(
    DISTINCTCOUNT('public fact_untersuchung'[PatientKey]),
    NOT(ISBLANK('public fact_untersuchung'[ICDKey])),
    'public dim_icd_code'[IsCurrent] = TRUE
)
```