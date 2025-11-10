# Sales Data ETL to Data Vault

ETL script to load `SalesData.csv` into a PostgreSQL Data Vault schema.

## Requirements

Install dependencies:

```powershell
pip install -r requirements.txt
```

## Database Setup

1. Ensure PostgreSQL is running
2. Create a database (or use an existing one like `postgres`)
3. Note your connection details: host, port, username, password, database name

## Usage

### Run ETL with custom PostgreSQL connection

```powershell
# Activate virtual environment (if using one)
D:/github/BI/.venv/Scripts/python.exe etl_salesdata.py --host localhost --port 5432 --user postgres --password "YourPassword" --db postgres --run-crebas
```

### Command-line arguments

- `--host`: PostgreSQL host (default: `localhost`)
- `--port`: PostgreSQL port (default: `5432`)
- `--user`: PostgreSQL username (default: `postgres`)
- `--password`: PostgreSQL password (default: `postgres`)
- `--db`: Database name (default: `postgres`)
- `--run-crebas`: Execute `crebas.sql` to (re)create schema before loading
- `--csv`: Path to SalesData.csv (default: `../Praktikum1/SalesData.csv`)
- `--crebas`: Path to crebas.sql (default: `./crebas.sql`)

### Examples

**First run (create schema and load data):**
```powershell
python etl_salesdata.py --host localhost --user myuser --password "mypass" --db mydatabase --run-crebas
```

**Subsequent runs (append data only):**
```powershell
python etl_salesdata.py --host localhost --user myuser --password "mypass" --db mydatabase
```

**Custom CSV path:**
```powershell
python etl_salesdata.py --csv "C:/data/SalesData.csv" --host localhost --user postgres --password "pass" --db postgres --run-crebas
```

## Data Transformations

### Country Normalization
- Input codes (GER, DEU, USA, U.S., etc.) → 2-letter codes (DE, US)
- Adds CountryName: DE → "Germany", US → "United States"

### Currency Normalization
- EUR, €, EURO → EUR
- USD → USD

### Data Vault Structure

**Hubs** (7 tables):
- HubCountry, HubCustomer, HubDate, HubFactSales, HubProduct, HubProductCategory, HubSalesOrg

**Links** (4 tables):
- LinkCustomerCountry, LinkProductProductCategory, LinkSalesOrgCountry, LinkFactSales

**Satellites** (7 tables):
- SatCountry, SatCustomer, SatDate, SatFactSales, SatProduct, SatProductCategory, SatSalesOrg

## Output

Upon successful completion, the script prints row counts:

```
ETL completed:
  Hubs: 175,706 rows across 7 tables
  Links: 171,067 rows across 4 tables
  Sats: 175,706 rows across 7 tables
```

## Troubleshooting

### Password with special characters

If your password contains `@`, `#`, or other special characters, wrap it in quotes:

```powershell
python etl_salesdata.py --password "MyP@ssw0rd!" --host localhost --db mydb --run-crebas
```

### Connection errors

- Verify PostgreSQL is running: `psql -U postgres -h localhost`
- Check firewall/port settings
- Confirm credentials are correct

### Schema already exists

Use `--run-crebas` to drop and recreate all tables (uses CASCADE to remove dependencies).
