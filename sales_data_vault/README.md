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

## Sales Mart (Star Schema)

After loading the Data Vault, you can create an analytical Sales Mart with a star schema.

### Create Sales Mart Schema

```powershell
psql -U postgres -d postgres -f sales_mart.sql
```

Or using pgAdmin, execute the `sales_mart.sql` script.

### Load Data into Sales Mart

Transform Data Vault into denormalized star schema:

```powershell
psql -U postgres -d postgres -f etl_dv_to_mart.sql
```

### Star Schema Structure

**Fact Table:**
- **FactSales**: Sales transactions with calculated measures (NetRevenue, GrossProfit, Margin)
  - Direct foreign keys to ALL dimensions (true star schema)

**Dimension Tables:**
- **DimDate**: Date dimension with calendar attributes (Year, Month, Quarter, Day, etc.)
- **DimCustomer**: Customer dimension (CustomerID, Name, City)
- **DimProduct**: Product dimension with category and division (denormalized)
- **DimSalesOrg**: Sales organization dimension
- **DimCountry**: Country dimension

**Star Schema Design:**
- All dimensions connect **directly** to the fact table
- Country is referenced from FactSales (not nested in Customer/SalesOrg)
- This ensures a true star schema (not snowflake)

**Features:**
- Pure star schema with all dimensions at same level
- Country referenced directly from fact table for maximum query performance
- Pre-calculated measures (NetRevenue, GrossProfit, GrossProfitMargin)
- Optimized indexes for common query patterns
- Foreign key constraints for referential integrity

### Example Queries

**Sales by Year and Country:**
```sql
SELECT 
    d.Year,
    co.CountryName,
    COUNT(*) AS Transactions,
    SUM(f.NetRevenueUSD) AS TotalRevenue,
    SUM(f.GrossProfit) AS TotalProfit
FROM FactSales f
JOIN DimDate d ON f.DateKey = d.DateKey
JOIN DimCountry co ON f.CountryKey = co.CountryKey
GROUP BY d.Year, co.CountryName
ORDER BY d.Year, TotalRevenue DESC;
```

**Top Products by Revenue:**
```sql
SELECT 
    p.ProductName,
    p.ProductCategoryName,
    COUNT(*) AS UnitsSold,
    SUM(f.NetRevenueUSD) AS TotalRevenue
FROM FactSales f
JOIN DimProduct p ON f.ProductKey = p.ProductKey
GROUP BY p.ProductName, p.ProductCategoryName
ORDER BY TotalRevenue DESC
LIMIT 10;
```

**Monthly Sales Trend:**
```sql
SELECT 
    d.Year,
    d.Month,
    d.MonthName,
    COUNT(*) AS Transactions,
    SUM(f.NetRevenueUSD) AS Revenue,
    AVG(f.GrossProfitMargin) AS AvgMargin
FROM FactSales f
JOIN DimDate d ON f.DateKey = d.DateKey
GROUP BY d.Year, d.Month, d.MonthName
ORDER BY d.Year, d.Month;
```
