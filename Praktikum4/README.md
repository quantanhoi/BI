# Praktikum 4 - Patient Data Mart

## Overview

This project creates a dimensional Data Mart from the Data Vault created in Praktikum 3. The Data Mart provides an optimized structure for analytical queries and reporting on patient data.

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                         Data Vault (Praktikum3)                   │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐   │
│  │    Hubs     │  │    Links    │  │       Satellites        │   │
│  │ Hub_Patient │  │Link_Patient_│  │ Sat_Patient_Stammdaten  │   │
│  │Hub_Untersuchung│ Untersuchung│  │ Sat_Untersuchung        │   │
│  │ Hub_ICD_*   │  │Link_Diagnose│  │ Sat_Messwerte           │   │
│  └─────────────┘  └─────────────┘  └─────────────────────────┘   │
└──────────────────────────────────────────────────────────────────┘
                              │
                              ▼ ETL
┌──────────────────────────────────────────────────────────────────┐
│                    Data Mart (Praktikum4)                        │
│  ┌─────────────────────┐       ┌─────────────────────────────┐   │
│  │    Dimensions       │       │          Facts              │   │
│  │ Dim_Patient         │       │ Fact_Untersuchung           │   │
│  │ Dim_ICD_Code        │       │ Fact_Patient_Anamnese       │   │
│  │ Dim_Date            │       │                             │   │
│  │ Dim_Praxis          │       │                             │   │
│  └─────────────────────┘       └─────────────────────────────┘   │
└──────────────────────────────────────────────────────────────────┘
```

## Data Mart Schema

### Dimension Tables

1. **Dim_Patient** - Patient dimension with master data
2. **Dim_ICD_Code** - ICD Code dimension with hierarchy (Kapitel → Gruppe → Code)
3. **Dim_Date** - Date dimension for time-based analysis
4. **Dim_Praxis** - Practice/Clinic dimension

### Fact Tables

1. **Fact_Untersuchung** - Examination facts with measurements
2. **Fact_Patient_Anamnese** - Patient medical history facts

## Files

| File | Description |
|------|-------------|
| `crebas.sql` | Data Mart schema creation script |
| `etl_dv_to_mart.sql` | ETL SQL scripts to load Data Mart from Data Vault |
| `docker-compose.yml` | Docker configuration for PostgreSQL |
| `analytical_queries.sql` | Sample analytical queries |

## Setup

1. Ensure Praktikum3 Data Vault is populated with data
2. Start the PostgreSQL container:
   ```bash
   docker-compose up -d
   ```
3. Create the Data Mart schema:
   ```bash
   docker exec -i datavault_postgres psql -U datavault_user -d datavault_db < crebas.sql
   ```
4. Run ETL to populate Data Mart:
   ```bash
   docker exec -i datavault_postgres psql -U datavault_user -d datavault_db < etl_dv_to_mart.sql
   ```

## Sample Queries

See `analytical_queries.sql` for example analytical queries including:
- Patient examination frequency analysis
- ICD code distribution by practice
- Time-based trend analysis
- Measurement statistics

## Dependencies

- PostgreSQL 16
- Docker & Docker Compose
- Data from Praktikum3 (Data Vault)
