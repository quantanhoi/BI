/*==============================================================*/
/* DBMS name:      PostgreSQL 9.x                               */
/* Created on:     10.11.2025 10:32:47                          */
/*==============================================================*/


drop index if exists HUBCOUNTRY_PK;

drop table if exists HubCountry;

drop index if exists HUBCUSTOMER_PK;

drop table if exists HubCustomer;

drop index if exists HUBDATE_PK;

drop table if exists HubDate;

drop index if exists HUBFACTSALES_PK;

drop table if exists HubFactSales;

drop index if exists HUBPRODUCT_PK;

drop table if exists HubProduct;

drop index if exists HUBPRODUCTCATEGORY_PK;

drop table if exists HubProductCategory;

drop index if exists HUBSALESORG_PK;

drop table if exists HubSalesOrg;

drop index if exists ASSOCIATION25_FK;

drop index if exists ASSOCIATION23_FK;

drop table if exists LinkCustomerCountry;

drop index if exists ASSOCIATION32_FK;

drop index if exists ASSOCIATION31_FK;

drop index if exists ASSOCIATION30_FK;

drop index if exists ASSOCIATION21_FK;

drop index if exists ASSOCIATION20_FK;

drop index if exists ASSOCIATION19_FK;

drop table if exists LinkFactSales;

drop index if exists ASSOCIATION17_FK;

drop index if exists ASSOCIATION16_FK;

drop table if exists LinkProductProductCategory;

drop index if exists ASSOCIATION28_FK;

drop index if exists ASSOCIATION27_FK;

drop table if exists LinkSalesOrgCountry;

drop index if exists ASSOCIATION24_FK;

drop index if exists SATCOUNTRY_PK;

drop table if exists SatCountry;

drop index if exists ASSOCIATION22_FK;

drop index if exists SATCUSTOMER_PK;

drop table if exists SatCustomer;

drop index if exists ASSOCIATION29_FK;

drop index if exists SATDATE_PK;

drop table if exists SatDate;

drop index if exists ASSOCIATION18_FK;

drop index if exists SATFACTSALES_PK;

drop table if exists SatFactSales;

drop index if exists ASSOCIATION15_FK;

drop index if exists SATPRODUCT_PK;

drop table if exists SatProduct;

drop index if exists ASSOCIATION14_FK;

drop index if exists SATPRODUCTCATEGORY_PK;

drop table if exists SatProductCategory;

drop index if exists ASSOCIATION26_FK;

drop index if exists SATSALESORG_PK;

drop table if exists SatSalesOrg;

/*==============================================================*/
/* Table: HubCountry                                            */
/*==============================================================*/
create table HubCountry (
   hubCountryId         INT4                 not null,
   loadDate             DATE                 null,
   sourceSystem         VARCHAR(254)         null,
   countryCode          VARCHAR(254)         null,
   constraint PK_HUBCOUNTRY primary key (hubCountryId)
);

/*==============================================================*/
/* Index: HUBCOUNTRY_PK                                         */
/*==============================================================*/
create unique index HUBCOUNTRY_PK on HubCountry (
hubCountryId
);

/*==============================================================*/
/* Table: HubCustomer                                           */
/*==============================================================*/
create table HubCustomer (
   hubCustomerId        INT4                 not null,
   loadDate             DATE                 null,
   sourceSystem         VARCHAR(254)         null,
   customerID           VARCHAR(254)         null,
   constraint PK_HUBCUSTOMER primary key (hubCustomerId)
);

/*==============================================================*/
/* Index: HUBCUSTOMER_PK                                        */
/*==============================================================*/
create unique index HUBCUSTOMER_PK on HubCustomer (
hubCustomerId
);

/*==============================================================*/
/* Table: HubDate                                               */
/*==============================================================*/
create table HubDate (
   hubDateId            INT4                 not null,
   loadDate             DATE                 null,
   sourceSystem         VARCHAR(254)         null,
   date                 INT4                 null,
   constraint PK_HUBDATE primary key (hubDateId)
);

/*==============================================================*/
/* Index: HUBDATE_PK                                            */
/*==============================================================*/
create unique index HUBDATE_PK on HubDate (
hubDateId
);

/*==============================================================*/
/* Table: HubFactSales                                          */
/*==============================================================*/
create table HubFactSales (
   hubFactSalesId       INT4                 not null,
   loadDate             DATE                 null,
   sourceSystem         VARCHAR(254)         null,
   orderItem            INT4                 null,
   orderNumber          INT4                 null,
   constraint PK_HUBFACTSALES primary key (hubFactSalesId)
);

/*==============================================================*/
/* Index: HUBFACTSALES_PK                                       */
/*==============================================================*/
create unique index HUBFACTSALES_PK on HubFactSales (
hubFactSalesId
);

/*==============================================================*/
/* Table: HubProduct                                            */
/*==============================================================*/
create table HubProduct (
   hubProductId         INT4                 not null,
   loadDate             DATE                 null,
   sourceSystem         VARCHAR(254)         null,
   productID            VARCHAR(254)         null,
   constraint PK_HUBPRODUCT primary key (hubProductId)
);

/*==============================================================*/
/* Index: HUBPRODUCT_PK                                         */
/*==============================================================*/
create unique index HUBPRODUCT_PK on HubProduct (
hubProductId
);

/*==============================================================*/
/* Table: HubProductCategory                                    */
/*==============================================================*/
create table HubProductCategory (
   hubProductCategoryId INT4                 not null,
   loadDate             DATE                 null,
   sourceSystem         VARCHAR(254)         null,
   productCatID         VARCHAR(254)         null,
   constraint PK_HUBPRODUCTCATEGORY primary key (hubProductCategoryId)
);

/*==============================================================*/
/* Index: HUBPRODUCTCATEGORY_PK                                 */
/*==============================================================*/
create unique index HUBPRODUCTCATEGORY_PK on HubProductCategory (
hubProductCategoryId
);

/*==============================================================*/
/* Table: HubSalesOrg                                           */
/*==============================================================*/
create table HubSalesOrg (
   hubSalesOrgId        INT4                 not null,
   loadDate             DATE                 null,
   sourceSystem         VARCHAR(254)         null,
   salesOrg             VARCHAR(254)         null,
   constraint PK_HUBSALESORG primary key (hubSalesOrgId)
);

/*==============================================================*/
/* Index: HUBSALESORG_PK                                        */
/*==============================================================*/
create unique index HUBSALESORG_PK on HubSalesOrg (
hubSalesOrgId
);

/*==============================================================*/
/* Table: LinkCustomerCountry                                   */
/*==============================================================*/
create table LinkCustomerCountry (
   hubCountryId         INT4                 not null,
   hubCustomerId        INT4                 not null,
   loadDate             DATE                 null,
   sourceSystem         VARCHAR(254)         null
);

/*==============================================================*/
/* Index: ASSOCIATION23_FK                                      */
/*==============================================================*/
create  index ASSOCIATION23_FK on LinkCustomerCountry (
hubCustomerId
);

/*==============================================================*/
/* Index: ASSOCIATION25_FK                                      */
/*==============================================================*/
create  index ASSOCIATION25_FK on LinkCustomerCountry (
hubCountryId
);

/*==============================================================*/
/* Table: LinkFactSales                                         */
/*==============================================================*/
create table LinkFactSales (
   hubProductId         INT4                 not null,
   hubCustomerId        INT4                 not null,
   hubDateId            INT4                 not null,
   hubFactSalesId       INT4                 not null,
   hubProductCategoryId INT4                 not null,
   hubSalesOrgId        INT4                 not null,
   loadDate             DATE                 null,
   sourceSystem         VARCHAR(254)         null
);

/*==============================================================*/
/* Index: ASSOCIATION19_FK                                      */
/*==============================================================*/
create  index ASSOCIATION19_FK on LinkFactSales (
hubFactSalesId
);

/*==============================================================*/
/* Index: ASSOCIATION20_FK                                      */
/*==============================================================*/
create  index ASSOCIATION20_FK on LinkFactSales (
hubProductCategoryId
);

/*==============================================================*/
/* Index: ASSOCIATION21_FK                                      */
/*==============================================================*/
create  index ASSOCIATION21_FK on LinkFactSales (
hubProductId
);

/*==============================================================*/
/* Index: ASSOCIATION30_FK                                      */
/*==============================================================*/
create  index ASSOCIATION30_FK on LinkFactSales (
hubDateId
);

/*==============================================================*/
/* Index: ASSOCIATION31_FK                                      */
/*==============================================================*/
create  index ASSOCIATION31_FK on LinkFactSales (
hubCustomerId
);

/*==============================================================*/
/* Index: ASSOCIATION32_FK                                      */
/*==============================================================*/
create  index ASSOCIATION32_FK on LinkFactSales (
hubSalesOrgId
);

/*==============================================================*/
/* Table: LinkProductProductCategory                            */
/*==============================================================*/
create table LinkProductProductCategory (
   hubProductId         INT4                 not null,
   hubProductCategoryId INT4                 not null,
   loadDate             DATE                 null,
   sourceSystem         VARCHAR(254)         null
);

/*==============================================================*/
/* Index: ASSOCIATION16_FK                                      */
/*==============================================================*/
create  index ASSOCIATION16_FK on LinkProductProductCategory (
hubProductCategoryId
);

/*==============================================================*/
/* Index: ASSOCIATION17_FK                                      */
/*==============================================================*/
create  index ASSOCIATION17_FK on LinkProductProductCategory (
hubProductId
);

/*==============================================================*/
/* Table: LinkSalesOrgCountry                                   */
/*==============================================================*/
create table LinkSalesOrgCountry (
   hubCountryId         INT4                 not null,
   hubSalesOrgId        INT4                 not null,
   loadDate             DATE                 null,
   sourceSystem         VARCHAR(254)         null
);

/*==============================================================*/
/* Index: ASSOCIATION27_FK                                      */
/*==============================================================*/
create  index ASSOCIATION27_FK on LinkSalesOrgCountry (
hubSalesOrgId
);

/*==============================================================*/
/* Index: ASSOCIATION28_FK                                      */
/*==============================================================*/
create  index ASSOCIATION28_FK on LinkSalesOrgCountry (
hubCountryId
);

/*==============================================================*/
/* Table: SatCountry                                            */
/*==============================================================*/
create table SatCountry (
   loadDate             DATE                 not null,
   hubCountryId         INT4                 not null,
   countryName          VARCHAR(254)         null,
   constraint PK_SATCOUNTRY primary key (loadDate, hubCountryId)
);

/*==============================================================*/
/* Index: SATCOUNTRY_PK                                         */
/*==============================================================*/
create unique index SATCOUNTRY_PK on SatCountry (
loadDate,
hubCountryId
);

/*==============================================================*/
/* Index: ASSOCIATION24_FK                                      */
/*==============================================================*/
create  index ASSOCIATION24_FK on SatCountry (
hubCountryId
);

/*==============================================================*/
/* Table: SatCustomer                                           */
/*==============================================================*/
create table SatCustomer (
   loadDate             DATE                 not null,
   hubCustomerId        INT4                 not null,
   custDescr            VARCHAR(254)         null,
   city                 VARCHAR(254)         null,
   constraint PK_SATCUSTOMER primary key (loadDate, hubCustomerId)
);

/*==============================================================*/
/* Index: SATCUSTOMER_PK                                        */
/*==============================================================*/
create unique index SATCUSTOMER_PK on SatCustomer (
loadDate,
hubCustomerId
);

/*==============================================================*/
/* Index: ASSOCIATION22_FK                                      */
/*==============================================================*/
create  index ASSOCIATION22_FK on SatCustomer (
hubCustomerId
);

/*==============================================================*/
/* Table: SatDate                                               */
/*==============================================================*/
create table SatDate (
   loadDate             DATE                 not null,
   hubDateId            INT4                 not null,
   year                 INT4                 null,
   month                INT4                 null,
   day                  INT4                 null,
   constraint PK_SATDATE primary key (loadDate, hubDateId)
);

/*==============================================================*/
/* Index: SATDATE_PK                                            */
/*==============================================================*/
create unique index SATDATE_PK on SatDate (
loadDate,
hubDateId
);

/*==============================================================*/
/* Index: ASSOCIATION29_FK                                      */
/*==============================================================*/
create  index ASSOCIATION29_FK on SatDate (
hubDateId
);

/*==============================================================*/
/* Table: SatFactSales                                          */
/*==============================================================*/
create table SatFactSales (
   loadDate             DATE                 not null,
   hubFactSalesId       INT4                 not null,
   salesQuantity        INT4                 null,
   UnitOfMeasure        VARCHAR(254)         null,
   RevenueUSD           NUMERIC              null,
   DiscountUSD          NUMERIC              null,
   CostsUSD             NUMERIC              null,
   Revenue              NUMERIC              null,
   Discount             NUMERIC              null,
   currency             VARCHAR(254)         null,
   constraint PK_SATFACTSALES primary key (loadDate, hubFactSalesId)
);

/*==============================================================*/
/* Index: SATFACTSALES_PK                                       */
/*==============================================================*/
create unique index SATFACTSALES_PK on SatFactSales (
loadDate,
hubFactSalesId
);

/*==============================================================*/
/* Index: ASSOCIATION18_FK                                      */
/*==============================================================*/
create  index ASSOCIATION18_FK on SatFactSales (
hubFactSalesId
);

/*==============================================================*/
/* Table: SatProduct                                            */
/*==============================================================*/
create table SatProduct (
   loadDate             DATE                 not null,
   hubProductId         INT4                 not null,
   prodDescr            VARCHAR(254)         null,
   divisionCode         VARCHAR(254)         null,
   constraint PK_SATPRODUCT primary key (loadDate, hubProductId)
);

/*==============================================================*/
/* Index: SATPRODUCT_PK                                         */
/*==============================================================*/
create unique index SATPRODUCT_PK on SatProduct (
loadDate,
hubProductId
);

/*==============================================================*/
/* Index: ASSOCIATION15_FK                                      */
/*==============================================================*/
create  index ASSOCIATION15_FK on SatProduct (
hubProductId
);

/*==============================================================*/
/* Table: SatProductCategory                                    */
/*==============================================================*/
create table SatProductCategory (
   catDescr             VARCHAR(254)         not null,
   loadDate             DATE                 not null,
   hubProductCategoryId INT4                 not null,
   constraint PK_SATPRODUCTCATEGORY primary key (loadDate, hubProductCategoryId)
);

/*==============================================================*/
/* Index: SATPRODUCTCATEGORY_PK                                 */
/*==============================================================*/
create unique index SATPRODUCTCATEGORY_PK on SatProductCategory (
loadDate,
hubProductCategoryId
);

/*==============================================================*/
/* Index: ASSOCIATION14_FK                                      */
/*==============================================================*/
create  index ASSOCIATION14_FK on SatProductCategory (
hubProductCategoryId
);

/*==============================================================*/
/* Table: SatSalesOrg                                           */
/*==============================================================*/
create table SatSalesOrg (
   loadDate             DATE                 not null,
   hubSalesOrgId        INT4                 not null,
   constraint PK_SATSALESORG primary key (loadDate, hubSalesOrgId)
);

/*==============================================================*/
/* Index: SATSALESORG_PK                                        */
/*==============================================================*/
create unique index SATSALESORG_PK on SatSalesOrg (
loadDate,
hubSalesOrgId
);

/*==============================================================*/
/* Index: ASSOCIATION26_FK                                      */
/*==============================================================*/
create  index ASSOCIATION26_FK on SatSalesOrg (
hubSalesOrgId
);

alter table LinkCustomerCountry
   add constraint FK_LINKCUST_ASSOCIATI_HUBCUSTO foreign key (hubCustomerId)
      references HubCustomer (hubCustomerId)
      on delete restrict on update restrict;

alter table LinkCustomerCountry
   add constraint FK_LINKCUST_ASSOCIATI_HUBCOUNT foreign key (hubCountryId)
      references HubCountry (hubCountryId)
      on delete restrict on update restrict;

alter table LinkFactSales
   add constraint FK_LINKFACT_ASSOCIATI_HUBFACTS foreign key (hubFactSalesId)
      references HubFactSales (hubFactSalesId)
      on delete restrict on update restrict;

alter table LinkFactSales
   add constraint FK_LINKFACT_ASSOCIATI_HUBPRODU foreign key (hubProductCategoryId)
      references HubProductCategory (hubProductCategoryId)
      on delete restrict on update restrict;

alter table LinkFactSales
   add constraint FK_LINKFACT_HUBPRODU foreign key (hubProductId)
      references HubProduct (hubProductId)
      on delete restrict on update restrict;

alter table LinkFactSales
   add constraint FK_LINKFACT_ASSOCIATI_HUBDATE foreign key (hubDateId)
      references HubDate (hubDateId)
      on delete restrict on update restrict;

alter table LinkFactSales
   add constraint FK_LINKFACT_ASSOCIATI_HUBCUSTO foreign key (hubCustomerId)
      references HubCustomer (hubCustomerId)
      on delete restrict on update restrict;

alter table LinkFactSales
   add constraint FK_LINKFACT_ASSOCIATI_HUBSALES foreign key (hubSalesOrgId)
      references HubSalesOrg (hubSalesOrgId)
      on delete restrict on update restrict;

alter table LinkProductProductCategory
   add constraint FK_LINKPROD_HUBPRODCAT foreign key (hubProductCategoryId)
      references HubProductCategory (hubProductCategoryId)
      on delete restrict on update restrict;

alter table LinkProductProductCategory
   add constraint FK_LINKPROD_ASSOCIATI_HUBPRODU foreign key (hubProductId)
      references HubProduct (hubProductId)
      on delete restrict on update restrict;

alter table LinkSalesOrgCountry
   add constraint FK_LINKSALE_ASSOCIATI_HUBSALES foreign key (hubSalesOrgId)
      references HubSalesOrg (hubSalesOrgId)
      on delete restrict on update restrict;

alter table LinkSalesOrgCountry
   add constraint FK_LINKSALE_ASSOCIATI_HUBCOUNT foreign key (hubCountryId)
      references HubCountry (hubCountryId)
      on delete restrict on update restrict;

alter table SatCountry
   add constraint FK_SATCOUNT_ASSOCIATI_HUBCOUNT foreign key (hubCountryId)
      references HubCountry (hubCountryId)
      on delete restrict on update restrict;

alter table SatCustomer
   add constraint FK_SATCUSTO_ASSOCIATI_HUBCUSTO foreign key (hubCustomerId)
      references HubCustomer (hubCustomerId)
      on delete restrict on update restrict;

alter table SatDate
   add constraint FK_SATDATE_ASSOCIATI_HUBDATE foreign key (hubDateId)
      references HubDate (hubDateId)
      on delete restrict on update restrict;

alter table SatFactSales
   add constraint FK_SATFACTS_ASSOCIATI_HUBFACTS foreign key (hubFactSalesId)
      references HubFactSales (hubFactSalesId)
      on delete restrict on update restrict;

alter table SatProduct
   add constraint FK_SATPRODU_ASSOCIATI_HUBPRODU foreign key (hubProductId)
      references HubProduct (hubProductId)
      on delete restrict on update restrict;

alter table SatProductCategory
   add constraint FK_SATPRODU_ASSOCIATI_HUBPRODU foreign key (hubProductCategoryId)
      references HubProductCategory (hubProductCategoryId)
      on delete restrict on update restrict;

alter table SatSalesOrg
   add constraint FK_SATSALES_ASSOCIATI_HUBSALES foreign key (hubSalesOrgId)
      references HubSalesOrg (hubSalesOrgId)
      on delete restrict on update restrict;

