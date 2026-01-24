-- Active: 1762767388693@@127.0.0.1@5432@postgres
/*==============================================================*/
/* DBMS name:      PostgreSQL 9.x                               */
/* Sales Mart - Star Schema                                     */
/* Created from Data Vault: Global Bike Sales                   */
/*==============================================================*/

-- Drop existing objects
DROP TABLE IF EXISTS FactSales CASCADE;
DROP TABLE IF EXISTS DimCustomer CASCADE;
DROP TABLE IF EXISTS DimProduct CASCADE;
DROP TABLE IF EXISTS DimDate CASCADE;
DROP TABLE IF EXISTS DimCountry CASCADE;
DROP TABLE IF EXISTS DimSalesOrg CASCADE;

/*==============================================================*/
/* Dimension: DimDate                                           */
/*==============================================================*/
CREATE TABLE DimDate (
   DateKey              INT4                 NOT NULL,
   Date                 DATE                 NOT NULL,
   Year                 INT4                 NOT NULL,
   Month                INT4                 NOT NULL,
   Day                  INT4                 NOT NULL,
   Quarter              INT4                 NOT NULL,
   MonthName            VARCHAR(20)          NOT NULL,
   DayOfWeek            INT4                 NOT NULL,
   DayName              VARCHAR(20)          NOT NULL,
   IsWeekend            BOOLEAN              NOT NULL,
   CONSTRAINT PK_DIMDATE PRIMARY KEY (DateKey)
);

CREATE INDEX IDX_DIMDATE_DATE ON DimDate(Date);
CREATE INDEX IDX_DIMDATE_YEAR_MONTH ON DimDate(Year, Month);

/*==============================================================*/
/* Dimension: DimCountry                                        */
/*==============================================================*/
CREATE TABLE DimCountry (
   CountryKey           INT4                 NOT NULL,
   CountryCode          VARCHAR(10)          NOT NULL,
   CountryName          VARCHAR(100)         NOT NULL,
   CONSTRAINT PK_DIMCOUNTRY PRIMARY KEY (CountryKey)
);

CREATE UNIQUE INDEX IDX_DIMCOUNTRY_CODE ON DimCountry(CountryCode);

/*==============================================================*/
/* Dimension: DimSalesOrg                                       */
/*==============================================================*/
CREATE TABLE DimSalesOrg (
   SalesOrgKey          INT4                 NOT NULL,
   SalesOrgID           VARCHAR(50)          NOT NULL,
   CONSTRAINT PK_DIMSALESORG PRIMARY KEY (SalesOrgKey)
);

CREATE UNIQUE INDEX IDX_DIMSALESORG_ID ON DimSalesOrg(SalesOrgID);

/*==============================================================*/
/* Dimension: DimCustomer                                       */
/*==============================================================*/
CREATE TABLE DimCustomer (
   CustomerKey          INT4                 NOT NULL,
   CustomerID           VARCHAR(50)          NOT NULL,
   CustomerName         VARCHAR(254)         NOT NULL,
   City                 VARCHAR(100)         NOT NULL,
   CONSTRAINT PK_DIMCUSTOMER PRIMARY KEY (CustomerKey)
);

CREATE UNIQUE INDEX IDX_DIMCUSTOMER_ID ON DimCustomer(CustomerID);

/*==============================================================*/
/* Dimension: DimProduct                                        */
/*==============================================================*/
CREATE TABLE DimProduct (
   ProductKey           INT4                 NOT NULL,
   ProductID            VARCHAR(50)          NOT NULL,
   ProductName          VARCHAR(254)         NOT NULL,
   ProductCategoryID    VARCHAR(50)          NOT NULL,
   ProductCategoryName  VARCHAR(100)         NOT NULL,
   Division             VARCHAR(50)          NOT NULL,
   CONSTRAINT PK_DIMPRODUCT PRIMARY KEY (ProductKey)
);

CREATE INDEX IDX_DIMPRODUCT_CATEGORY ON DimProduct(ProductCategoryID);
CREATE UNIQUE INDEX IDX_DIMPRODUCT_ID ON DimProduct(ProductID);

/*==============================================================*/
/* Fact Table: FactSales                                        */
/*==============================================================*/
CREATE TABLE FactSales (
   SalesKey             SERIAL               NOT NULL,
   DateKey              INT4                 NOT NULL,
   CustomerKey          INT4                 NOT NULL,
   ProductKey           INT4                 NOT NULL,
   SalesOrgKey          INT4                 NOT NULL,
   CountryKey           INT4                 NOT NULL,
   OrderNumber          INT4                 NOT NULL,
   OrderItem            INT4                 NOT NULL,
   SalesQuantity        INT4                 NOT NULL,
   UnitOfMeasure        VARCHAR(10)          NOT NULL,
   Revenue              NUMERIC(15,2)        NOT NULL,
   Discount             NUMERIC(15,2)        NOT NULL,
   RevenueUSD           NUMERIC(15,2)        NOT NULL,
   DiscountUSD          NUMERIC(15,2)        NOT NULL,
   CostsUSD             NUMERIC(15,2)        NOT NULL,
   Currency             VARCHAR(10)          NOT NULL,
   NetRevenue           NUMERIC(15,2)        NOT NULL,
   NetRevenueUSD        NUMERIC(15,2)        NOT NULL,
   GrossProfit          NUMERIC(15,2)        NOT NULL,
   GrossProfitMargin    NUMERIC(5,2)         NULL,
   CONSTRAINT PK_FACTSALES PRIMARY KEY (SalesKey)
);

-- Foreign Key Constraints
ALTER TABLE FactSales
   ADD CONSTRAINT FK_FACTSALES_DATE 
   FOREIGN KEY (DateKey) 
   REFERENCES DimDate(DateKey);

ALTER TABLE FactSales
   ADD CONSTRAINT FK_FACTSALES_CUSTOMER 
   FOREIGN KEY (CustomerKey) 
   REFERENCES DimCustomer(CustomerKey);

ALTER TABLE FactSales
   ADD CONSTRAINT FK_FACTSALES_PRODUCT 
   FOREIGN KEY (ProductKey) 
   REFERENCES DimProduct(ProductKey);

ALTER TABLE FactSales
   ADD CONSTRAINT FK_FACTSALES_SALESORG 
   FOREIGN KEY (SalesOrgKey) 
   REFERENCES DimSalesOrg(SalesOrgKey);

ALTER TABLE FactSales
   ADD CONSTRAINT FK_FACTSALES_COUNTRY 
   FOREIGN KEY (CountryKey) 
   REFERENCES DimCountry(CountryKey);

-- Performance Indexes on Fact Table
CREATE INDEX IDX_FACTSALES_DATE ON FactSales(DateKey);
CREATE INDEX IDX_FACTSALES_CUSTOMER ON FactSales(CustomerKey);
CREATE INDEX IDX_FACTSALES_PRODUCT ON FactSales(ProductKey);
CREATE INDEX IDX_FACTSALES_SALESORG ON FactSales(SalesOrgKey);
CREATE INDEX IDX_FACTSALES_COUNTRY ON FactSales(CountryKey);
CREATE INDEX IDX_FACTSALES_ORDER ON FactSales(OrderNumber, OrderItem);
CREATE INDEX IDX_FACTSALES_DATE_PRODUCT ON FactSales(DateKey, ProductKey);
CREATE INDEX IDX_FACTSALES_DATE_CUSTOMER ON FactSales(DateKey, CustomerKey);
CREATE INDEX IDX_FACTSALES_DATE_COUNTRY ON FactSales(DateKey, CountryKey);

/*==============================================================*/
/* Comments                                                     */
/*==============================================================*/
COMMENT ON TABLE DimDate IS 'Date dimension with calendar attributes';
COMMENT ON TABLE DimCountry IS 'Country dimension - connected directly to fact table (star schema)';
COMMENT ON TABLE DimSalesOrg IS 'Sales organization dimension';
COMMENT ON TABLE DimCustomer IS 'Customer dimension';
COMMENT ON TABLE DimProduct IS 'Product dimension with category denormalization';
COMMENT ON TABLE FactSales IS 'Sales fact table with direct references to all dimensions (star schema)';

COMMENT ON COLUMN FactSales.CountryKey IS 'Direct reference to country dimension (star schema pattern)';
COMMENT ON COLUMN FactSales.NetRevenue IS 'Revenue - Discount in original currency';
COMMENT ON COLUMN FactSales.NetRevenueUSD IS 'Revenue - Discount in USD';
COMMENT ON COLUMN FactSales.GrossProfit IS 'Net Revenue USD - Costs USD';
COMMENT ON COLUMN FactSales.GrossProfitMargin IS 'Gross Profit / Net Revenue USD * 100';
