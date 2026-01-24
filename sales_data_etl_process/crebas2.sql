drop index if exists COUNTRY_PK;
drop table if exists Country cascade;

drop index if exists ASSOCIATION7_FK;
drop index if exists CUSTOMER_PK;
drop table if exists Customer cascade;

drop index if exists DATE_PK;
drop table if exists "Date" cascade;

drop index if exists ASSOCIATION11_FK;
drop index if exists ASSOCIATION10_FK;
drop index if exists ASSOCIATION9_FK;
drop index if exists ASSOCIATION8_FK;
drop index if exists FACTSALES_PK;
drop table if exists FactSales cascade;

drop index if exists ASSOCIATION13_FK;
drop index if exists ORDER_PK;
drop table if exists "Order" cascade;

drop index if exists ASSOCIATION12_FK;
drop index if exists PRODUCT_PK;
drop table if exists Product cascade;

drop index if exists PRODUCTCATEGORY_PK;
drop table if exists ProductCategory cascade;

drop index if exists SALESORG_PK;
drop table if exists SalesOrg cascade;

-- CREATE Statements
create table Country (
    countryCode          VARCHAR(254)                   not null,
    countryName          VARCHAR(254),
    primary key (countryCode)
);

create unique index COUNTRY_PK on Country (
    countryCode ASC
);

create table Customer (
    customerID           INTEGER                        not null,
    countryCode          VARCHAR(254),
    custDescr            VARCHAR(254),
    city                 VARCHAR(254),
    primary key (customerID),
    foreign key (countryCode)
        references Country (countryCode)
);

create unique index CUSTOMER_PK on Customer (
    customerID ASC
);

create index ASSOCIATION7_FK on Customer (
    countryCode ASC
);

create table "Date" (
    dateID               INTEGER                        not null,
    "date"               DATE,
    year                 INTEGER,
    month                INTEGER,
    day                  INTEGER,
    primary key (dateID)
);

create unique index DATE_PK on "Date" (
    dateID ASC
);

create table SalesOrg (
    salesOrgID           VARCHAR(254)                   not null,
    salesOrgCode         VARCHAR(254),
    primary key (salesOrgID)
);

create unique index SALESORG_PK on SalesOrg (
    salesOrgID ASC
);

create table "Order" (
    orderNumber          INTEGER                        not null,
    salesOrgID           VARCHAR(254),
    currency             VARCHAR(254),
    revenue              NUMERIC,
    discount             NUMERIC,
    primary key (orderNumber),
    foreign key (salesOrgID)
        references SalesOrg (salesOrgID)
);

create unique index ORDER_PK on "Order" (
    orderNumber ASC
);

create index ASSOCIATION13_FK on "Order" (
    salesOrgID ASC
);

create table ProductCategory (
    catDescr             VARCHAR(254),
    prodCatID            VARCHAR(254)                   not null,
    primary key (prodCatID)
);

create unique index PRODUCTCATEGORY_PK on ProductCategory (
    prodCatID ASC
);

create table Product (
    productID            VARCHAR(254)                   not null,
    prodCatID            VARCHAR(254),
    prodDescr            VARCHAR(254),
    divisionCode         VARCHAR(254),
    primary key (productID),
    foreign key (prodCatID)
        references ProductCategory (prodCatID)
);

create unique index PRODUCT_PK on Product (
    productID ASC
);

create index ASSOCIATION12_FK on Product (
    prodCatID ASC
);

create table FactSales (
    orderItem            VARCHAR(254)                   not null,
    productID            VARCHAR(254),
    customerID           INTEGER,
    orderNumber          INTEGER,
    dateID               INTEGER,
    salesQuantity        INTEGER,
    unitOfMeasure        VARCHAR(254),
    revenueUSD           NUMERIC,
    discountUSD          NUMERIC,
    costsUSD             NUMERIC,
    primary key (orderItem),
    foreign key (dateID)
        references "Date" (dateID),
    foreign key (orderNumber)
        references "Order" (orderNumber),
    foreign key (productID)
        references Product (productID),
    foreign key (customerID)
        references Customer (customerID)
);

create unique index FACTSALES_PK on FactSales (
    orderItem ASC
);

create index ASSOCIATION8_FK on FactSales (
    dateID ASC
);

create index ASSOCIATION9_FK on FactSales (
    orderNumber ASC
);

create index ASSOCIATION10_FK on FactSales (
    productID ASC
);

create index ASSOCIATION11_FK on FactSales (
    customerID ASC
);