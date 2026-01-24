-- Active: 1762767388693@@127.0.0.1@5432@postgres
/*==============================================================*/
/* ETL Script: Data Vault to Sales Mart                         */
/* Transforms normalized Data Vault into denormalized Star Schema */
/*==============================================================*/

-- Truncate mart tables before loading
TRUNCATE TABLE FactSales CASCADE;
TRUNCATE TABLE DimDate CASCADE;
TRUNCATE TABLE DimCountry CASCADE;
TRUNCATE TABLE DimSalesOrg CASCADE;
TRUNCATE TABLE DimCustomer CASCADE;
TRUNCATE TABLE DimProduct CASCADE;

/*==============================================================*/
/* Load DimDate                                                 */
/*==============================================================*/
INSERT INTO DimDate (
    DateKey,
    Date,
    Year,
    Month,
    Day,
    Quarter,
    MonthName,
    DayOfWeek,
    DayName,
    IsWeekend
)
SELECT DISTINCT
    h.hubDateId AS DateKey,
    TO_DATE(h.date::TEXT, 'YYYYMMDD') AS Date,
    COALESCE(s.year, EXTRACT(YEAR FROM TO_DATE(h.date::TEXT, 'YYYYMMDD'))) AS Year,
    COALESCE(s.month, EXTRACT(MONTH FROM TO_DATE(h.date::TEXT, 'YYYYMMDD'))) AS Month,
    COALESCE(s.day, EXTRACT(DAY FROM TO_DATE(h.date::TEXT, 'YYYYMMDD'))) AS Day,
    CEIL(COALESCE(s.month, EXTRACT(MONTH FROM TO_DATE(h.date::TEXT, 'YYYYMMDD')))::NUMERIC / 3) AS Quarter,
    TO_CHAR(TO_DATE(h.date::TEXT, 'YYYYMMDD'), 'Month') AS MonthName,
    EXTRACT(DOW FROM TO_DATE(h.date::TEXT, 'YYYYMMDD')) AS DayOfWeek,
    TO_CHAR(TO_DATE(h.date::TEXT, 'YYYYMMDD'), 'Day') AS DayName,
    CASE WHEN EXTRACT(DOW FROM TO_DATE(h.date::TEXT, 'YYYYMMDD')) IN (0, 6) 
         THEN TRUE 
         ELSE FALSE 
    END AS IsWeekend
FROM HubDate h
LEFT JOIN SatDate s ON h.hubDateId = s.hubDateId
WHERE h.date IS NOT NULL;

/*==============================================================*/
/* Load DimCountry                                              */
/*==============================================================*/
INSERT INTO DimCountry (
    CountryKey,
    CountryCode,
    CountryName
)
SELECT DISTINCT
    h.hubCountryId AS CountryKey,
    h.countryCode AS CountryCode,
    COALESCE(s.countryName, h.countryCode) AS CountryName
FROM HubCountry h
LEFT JOIN SatCountry s ON h.hubCountryId = s.hubCountryId
WHERE h.countryCode IS NOT NULL;

/*==============================================================*/
/* Load DimSalesOrg                                             */
/*==============================================================*/
INSERT INTO DimSalesOrg (
    SalesOrgKey,
    SalesOrgID
)
SELECT DISTINCT
    hso.hubSalesOrgId AS SalesOrgKey,
    hso.salesOrg AS SalesOrgID
FROM HubSalesOrg hso
WHERE hso.salesOrg IS NOT NULL;

/*==============================================================*/
/* Load DimCustomer                                             */
/*==============================================================*/
INSERT INTO DimCustomer (
    CustomerKey,
    CustomerID,
    CustomerName,
    City
)
SELECT DISTINCT
    hcust.hubCustomerId AS CustomerKey,
    hcust.customerID AS CustomerID,
    COALESCE(scust.custDescr, 'Unknown') AS CustomerName,
    COALESCE(scust.city, 'Unknown') AS City
FROM HubCustomer hcust
LEFT JOIN SatCustomer scust ON hcust.hubCustomerId = scust.hubCustomerId
WHERE hcust.customerID IS NOT NULL;

/*==============================================================*/
/* Load DimProduct                                              */
/*==============================================================*/
INSERT INTO DimProduct (
    ProductKey,
    ProductID,
    ProductName,
    ProductCategoryID,
    ProductCategoryName,
    Division
)
SELECT DISTINCT
    hp.hubProductId AS ProductKey,
    hp.productID AS ProductID,
    COALESCE(sp.prodDescr, 'Unknown') AS ProductName,
    hpc.productCatID AS ProductCategoryID,
    COALESCE(spc.catDescr, 'Unknown') AS ProductCategoryName,
    COALESCE(sp.divisionCode, 'Unknown') AS Division
FROM HubProduct hp
INNER JOIN LinkProductProductCategory lppc ON hp.hubProductId = lppc.hubProductId
INNER JOIN HubProductCategory hpc ON lppc.hubProductCategoryId = hpc.hubProductCategoryId
LEFT JOIN SatProduct sp ON hp.hubProductId = sp.hubProductId
LEFT JOIN SatProductCategory spc ON hpc.hubProductCategoryId = spc.hubProductCategoryId
WHERE hp.productID IS NOT NULL;

/*==============================================================*/
/* Load FactSales                                               */
/*==============================================================*/
INSERT INTO FactSales (
    DateKey,
    CustomerKey,
    ProductKey,
    SalesOrgKey,
    CountryKey,
    OrderNumber,
    OrderItem,
    SalesQuantity,
    UnitOfMeasure,
    Revenue,
    Discount,
    RevenueUSD,
    DiscountUSD,
    CostsUSD,
    Currency,
    NetRevenue,
    NetRevenueUSD,
    GrossProfit,
    GrossProfitMargin
)
SELECT
    lfs.hubDateId AS DateKey,
    lfs.hubCustomerId AS CustomerKey,
    lfs.hubProductId AS ProductKey,
    lfs.hubSalesOrgId AS SalesOrgKey,
    -- Get CountryKey from LinkSalesOrgCountry
    lsoc.hubCountryId AS CountryKey,
    hfs.orderNumber,
    hfs.orderItem,
    COALESCE(sfs.salesQuantity, 0) AS SalesQuantity,
    COALESCE(sfs.UnitOfMeasure, 'ST') AS UnitOfMeasure,
    COALESCE(sfs.Revenue, 0) AS Revenue,
    COALESCE(sfs.Discount, 0) AS Discount,
    COALESCE(sfs.RevenueUSD, 0) AS RevenueUSD,
    COALESCE(sfs.DiscountUSD, 0) AS DiscountUSD,
    COALESCE(sfs.CostsUSD, 0) AS CostsUSD,
    COALESCE(sfs.currency, 'EUR') AS Currency,
    -- Calculated measures
    COALESCE(sfs.Revenue, 0) - COALESCE(sfs.Discount, 0) AS NetRevenue,
    COALESCE(sfs.RevenueUSD, 0) - COALESCE(sfs.DiscountUSD, 0) AS NetRevenueUSD,
    (COALESCE(sfs.RevenueUSD, 0) - COALESCE(sfs.DiscountUSD, 0)) - COALESCE(sfs.CostsUSD, 0) AS GrossProfit,
    CASE 
        WHEN (COALESCE(sfs.RevenueUSD, 0) - COALESCE(sfs.DiscountUSD, 0)) > 0 
        THEN (((COALESCE(sfs.RevenueUSD, 0) - COALESCE(sfs.DiscountUSD, 0)) - COALESCE(sfs.CostsUSD, 0)) 
              / (COALESCE(sfs.RevenueUSD, 0) - COALESCE(sfs.DiscountUSD, 0))) * 100
        ELSE NULL
    END AS GrossProfitMargin
FROM LinkFactSales lfs
INNER JOIN HubFactSales hfs ON lfs.hubFactSalesId = hfs.hubFactSalesId
INNER JOIN LinkSalesOrgCountry lsoc ON lfs.hubSalesOrgId = lsoc.hubSalesOrgId
LEFT JOIN SatFactSales sfs ON hfs.hubFactSalesId = sfs.hubFactSalesId
WHERE lfs.hubDateId IS NOT NULL
  AND lfs.hubCustomerId IS NOT NULL
  AND lfs.hubProductId IS NOT NULL
  AND lfs.hubSalesOrgId IS NOT NULL
  AND lsoc.hubCountryId IS NOT NULL
ORDER BY hfs.orderNumber, hfs.orderItem;

/*==============================================================*/
/* Data Quality Checks                                          */
/*==============================================================*/

-- Check for orphaned records
SELECT 'DimDate' AS Dimension, COUNT(*) AS RecordCount FROM DimDate
UNION ALL
SELECT 'DimCountry', COUNT(*) FROM DimCountry
UNION ALL
SELECT 'DimSalesOrg', COUNT(*) FROM DimSalesOrg
UNION ALL
SELECT 'DimCustomer', COUNT(*) FROM DimCustomer
UNION ALL
SELECT 'DimProduct', COUNT(*) FROM DimProduct
UNION ALL
SELECT 'FactSales', COUNT(*) FROM FactSales;

-- Verify fact table totals
SELECT 
    COUNT(*) AS TotalTransactions,
    SUM(SalesQuantity) AS TotalQuantity,
    ROUND(SUM(NetRevenueUSD), 2) AS TotalNetRevenueUSD,
    ROUND(SUM(GrossProfit), 2) AS TotalGrossProfit,
    ROUND(AVG(GrossProfitMargin), 2) AS AvgGrossProfitMargin
FROM FactSales;

-- Summary by Year
SELECT 
    d.Year,
    COUNT(*) AS Transactions,
    ROUND(SUM(f.NetRevenueUSD), 2) AS NetRevenueUSD,
    ROUND(SUM(f.GrossProfit), 2) AS GrossProfit
FROM FactSales f
INNER JOIN DimDate d ON f.DateKey = d.DateKey
GROUP BY d.Year
ORDER BY d.Year;
