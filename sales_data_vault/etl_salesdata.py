import argparse
import os
from datetime import date
from urllib.parse import quote_plus

import pandas as pd
from sqlalchemy import create_engine, text


def normalize_country_code(raw: str) -> str:
    """Normalize country codes to 2-letter format (DE, US)."""
    if pd.isna(raw):
        return None
    s = str(raw).strip().upper()
    mapping = {
        'GER': 'DE',
        'DEU': 'DE',
        'DE': 'DE',
        'USA': 'US',
        'U.S.': 'US',
        'US': 'US',
    }
    return mapping.get(s, s)


def country_name_from_code(code: str) -> str:
    """Map 2-letter country code to full name."""
    if code is None:
        return None
    names = {
        'DE': 'Germany',
        'US': 'United States',
    }
    return names.get(code, code)


def normalize_currency(raw: str) -> str:
    """Normalize currency: EUR/€ -> EUR, USD -> USD."""
    if pd.isna(raw):
        return None
    s = str(raw).strip().upper()
    euro_aliases = {'EUR', '€', 'EURO'}
    if s in euro_aliases:
        return 'EUR'
    if s == 'USD':
        return 'USD'
    return s


def run_sql_file(engine, sql_path: str):
    """Execute all statements in a SQL file."""
    with open(sql_path, 'r', encoding='utf-8') as f:
        sql_text = f.read()
    statements = [stmt.strip() for stmt in sql_text.split(';') if stmt.strip()]
    with engine.begin() as conn:
        for stmt in statements:
            conn.execute(text(stmt))


def factorize_series(series: pd.Series, start: int = 1) -> pd.Series:
    """Assign stable integer IDs to unique values in a series."""
    codes, uniques = pd.factorize(series, sort=True)
    return pd.Series(codes + start, index=series.index)


def build_hubs(df: pd.DataFrame) -> dict:
    """Build all Hub tables from source DataFrame."""
    hubs = {}

    # HubCountry
    df['CountryCodeNorm'] = df['Country'].map(normalize_country_code)
    hubs['hubcountry'] = (
        df[['CountryCodeNorm']]
        .drop_duplicates()
        .assign(hubCountryId=lambda d: factorize_series(d['CountryCodeNorm']))
        .rename(columns={'CountryCodeNorm': 'countryCode'})
    )[['hubCountryId', 'countryCode']]

    # HubCustomer
    hubs['hubcustomer'] = (
        df[['Customer']]
        .drop_duplicates()
        .assign(hubCustomerId=lambda d: factorize_series(d['Customer']))
        .rename(columns={'Customer': 'customerID'})
    )[['hubCustomerId', 'customerID']]

    # HubDate
    df['DateParsed'] = pd.to_datetime(df['Date'], format='%d.%m.%y', errors='coerce')
    df['DateKey'] = df['DateParsed'].dt.strftime('%Y%m%d').astype('Int64')
    hubs['hubdate'] = (
        df[['DateKey']]
        .drop_duplicates()
        .assign(hubDateId=lambda d: factorize_series(d['DateKey']))
        .rename(columns={'DateKey': 'date'})
    )[['hubDateId', 'date']]

    # HubFactSales
    df['FactKey'] = df['OrderNumber'].astype('string') + '-' + df['OrderItem'].astype('string')
    hubs['hubfactsales'] = (
        df[['FactKey', 'OrderNumber', 'OrderItem']]
        .drop_duplicates()
        .assign(hubFactSalesId=lambda d: factorize_series(d['FactKey']))
        .rename(columns={'OrderNumber': 'orderNumber', 'OrderItem': 'orderItem'})
    )[['hubFactSalesId', 'orderItem', 'orderNumber']]

    # HubProduct
    hubs['hubproduct'] = (
        df[['Product']]
        .drop_duplicates()
        .assign(hubProductId=lambda d: factorize_series(d['Product']))
        .rename(columns={'Product': 'productID'})
    )[['hubProductId', 'productID']]

    # HubProductCategory
    hubs['hubproductcategory'] = (
        df[['ProdCat']]
        .drop_duplicates()
        .assign(hubProductCategoryId=lambda d: factorize_series(d['ProdCat']))
        .rename(columns={'ProdCat': 'productCatID'})
    )[['hubProductCategoryId', 'productCatID']]

    # HubSalesOrg
    hubs['hubsalesorg'] = (
        df[['SalesOrg']]
        .drop_duplicates()
        .assign(hubSalesOrgId=lambda d: factorize_series(d['SalesOrg']))
        .rename(columns={'SalesOrg': 'salesOrg'})
    )[['hubSalesOrgId', 'salesOrg']]

    return hubs


def build_links(df: pd.DataFrame, hubs: dict) -> dict:
    """Build all Link tables from source DataFrame and Hubs."""
    links = {}

    country_lu = hubs['hubcountry']
    cust_lu = hubs['hubcustomer']
    date_lu = hubs['hubdate']
    prod_lu = hubs['hubproduct']
    cat_lu = hubs['hubproductcategory']
    salesorg_lu = hubs['hubsalesorg']

    # Local FactKey -> hubFactSalesId mapping
    fact_map = (
        df[['FactKey']]
        .drop_duplicates()
        .assign(hubFactSalesId=lambda d: factorize_series(d['FactKey']))
    )

    base = pd.DataFrame({
        'CountryCodeNorm': df['Country'].map(normalize_country_code),
        'Customer': df['Customer'],
        'DateKey': df['DateParsed'].dt.strftime('%Y%m%d').astype('Int64'),
        'FactKey': df['FactKey'],
        'Product': df['Product'],
        'ProdCat': df['ProdCat'],
        'SalesOrg': df['SalesOrg'],
    })

    # LinkCustomerCountry
    links['linkcustomercountry'] = (
        base[['CountryCodeNorm', 'Customer']]
        .drop_duplicates()
        .merge(country_lu, left_on='CountryCodeNorm', right_on='countryCode')
        .merge(cust_lu, left_on='Customer', right_on='customerID')
    )[['hubCountryId', 'hubCustomerId']]

    # LinkProductProductCategory
    links['linkproductproductcategory'] = (
        base[['Product', 'ProdCat']]
        .drop_duplicates()
        .merge(prod_lu, left_on='Product', right_on='productID')
        .merge(cat_lu, left_on='ProdCat', right_on='productCatID')
    )[['hubProductId', 'hubProductCategoryId']]

    # LinkSalesOrgCountry
    links['linksalesorgcountry'] = (
        base[['SalesOrg', 'CountryCodeNorm']]
        .drop_duplicates()
        .merge(salesorg_lu, left_on='SalesOrg', right_on='salesOrg')
        .merge(country_lu, left_on='CountryCodeNorm', right_on='countryCode')
    )[['hubCountryId', 'hubSalesOrgId']]

    # LinkFactSales
    links['linkfactsales'] = (
        base[['Product', 'Customer', 'DateKey', 'FactKey', 'ProdCat', 'SalesOrg']]
        .drop_duplicates()
        .merge(prod_lu, left_on='Product', right_on='productID')
        .merge(cust_lu, left_on='Customer', right_on='customerID')
        .merge(date_lu, left_on='DateKey', right_on='date')
        .merge(fact_map, on='FactKey')
        .merge(cat_lu, left_on='ProdCat', right_on='productCatID')
        .merge(salesorg_lu, left_on='SalesOrg', right_on='salesOrg')
    )[['hubProductId', 'hubCustomerId', 'hubDateId', 'hubFactSalesId', 'hubProductCategoryId', 'hubSalesOrgId']]

    return links


def build_sats(df: pd.DataFrame, hubs: dict, load_dt: date) -> dict:
    """Build all Satellite tables from source DataFrame and Hubs."""
    sats = {}
    ld = pd.Timestamp(load_dt)

    country_lu = hubs['hubcountry']
    cust_lu = hubs['hubcustomer']
    date_lu = hubs['hubdate']
    prod_lu = hubs['hubproduct']
    cat_lu = hubs['hubproductcategory']
    salesorg_lu = hubs['hubsalesorg']

    # Local FactKey mapping
    fact_map = (
        df[['FactKey']]
        .drop_duplicates()
        .assign(hubFactSalesId=lambda d: factorize_series(d['FactKey']))
    )

    # SatCountry
    sats['satcountry'] = (
        country_lu.assign(loadDate=ld)
        .assign(countryName=lambda d: d['countryCode'].map(country_name_from_code))
    )[['loadDate', 'hubCountryId', 'countryName']].drop_duplicates()

    # SatCustomer
    sats['satcustomer'] = (
        df[['Customer', 'CustDescr', 'City']]
        .drop_duplicates()
        .merge(cust_lu, left_on='Customer', right_on='customerID')
        .assign(loadDate=ld)
        .rename(columns={'CustDescr': 'custDescr', 'City': 'city'})
    )[['loadDate', 'hubCustomerId', 'custDescr', 'city']]

    # SatDate
    sats['satdate'] = (
        df[['DateKey', 'DateParsed']]
        .drop_duplicates()
        .merge(date_lu, left_on='DateKey', right_on='date')
        .assign(loadDate=ld)
        .assign(year=lambda d: d['DateParsed'].dt.year.astype('Int64'))
        .assign(month=lambda d: d['DateParsed'].dt.month.astype('Int64'))
        .assign(day=lambda d: d['DateParsed'].dt.day.astype('Int64'))
    )[['loadDate', 'hubDateId', 'year', 'month', 'day']]

    # SatFactSales
    df['CurrencyNorm'] = df['Currency'].map(normalize_currency)
    sats['satfactsales'] = (
        df[['FactKey', 'SalesQuantity', 'UnitOfMeasure', 'RevenueUSD', 'DiscountUSD', 'CostsUSD', 'Revenue', 'Discount', 'CurrencyNorm']]
        .merge(fact_map, on='FactKey')
        .assign(loadDate=ld)
        .rename(columns={
            'SalesQuantity': 'salesQuantity',
            'CurrencyNorm': 'currency',
        })
    )[['loadDate', 'hubFactSalesId', 'salesQuantity', 'UnitOfMeasure', 'RevenueUSD', 'DiscountUSD', 'CostsUSD', 'Revenue', 'Discount', 'currency']]

    # SatProduct
    sats['satproduct'] = (
        df[['Product', 'ProdDescr', 'Division']]
        .drop_duplicates()
        .merge(prod_lu, left_on='Product', right_on='productID')
        .assign(loadDate=ld)
        .rename(columns={'ProdDescr': 'prodDescr', 'Division': 'divisionCode'})
    )[['loadDate', 'hubProductId', 'prodDescr', 'divisionCode']]

    # SatProductCategory
    sats['satproductcategory'] = (
        df[['ProdCat', 'CatDescr']]
        .drop_duplicates()
        .merge(cat_lu, left_on='ProdCat', right_on='productCatID')
        .assign(loadDate=ld)
        .rename(columns={'CatDescr': 'catDescr'})
    )[['catDescr', 'loadDate', 'hubProductCategoryId']]

    # SatSalesOrg
    sats['satsalesorg'] = (
        salesorg_lu.assign(loadDate=ld)
    )[['loadDate', 'hubSalesOrgId']].drop_duplicates()

    return sats


def write_tables(engine, tables: dict):
    """Write DataFrames to Postgres, lowercasing column names."""
    with engine.begin() as conn:
        for table_name, df in tables.items():
            if not df.empty:
                df_to_write = df.copy()
                df_to_write.columns = [str(c).lower() for c in df_to_write.columns]
                df_to_write.to_sql(table_name, con=conn, if_exists='append', index=False, method='multi', chunksize=5000)


def main():
    parser = argparse.ArgumentParser(description='ETL SalesData.csv to Postgres (Data Vault schema).')
    parser.add_argument('--csv', default=os.path.join('..', 'Praktikum1', 'SalesData.csv'), help='Path to SalesData.csv')
    parser.add_argument('--host', default='localhost', help='Postgres host')
    parser.add_argument('--port', default='5432', help='Postgres port')
    parser.add_argument('--user', default='postgres', help='Postgres username')
    parser.add_argument('--password', default='postgres', help='Postgres password')
    parser.add_argument('--db', default='postgres', help='Postgres database name')
    parser.add_argument('--run-crebas', action='store_true', help='Execute crebas.sql before loading')
    parser.add_argument('--crebas', default=os.path.join(os.path.dirname(__file__), 'crebas.sql'), help='Path to crebas.sql')
    args = parser.parse_args()

    # Build connection string from components (URL-encode password to handle special chars like @)
    conn_str = f'postgresql+psycopg2://{args.user}:{quote_plus(args.password)}@{args.host}:{args.port}/{args.db}'

    # Read CSV
    df = pd.read_csv(args.csv, sep=';', decimal=',', engine='python', on_bad_lines='skip')
    if 'Customer' in df.columns:
        df['Customer'] = df['Customer'].astype('string')

    # Build hubs, links, sats
    hubs = build_hubs(df)
    links = build_links(df, hubs)
    sats = build_sats(df, hubs, load_dt=date.today())

    # Connect to DB
    engine = create_engine(conn_str)

    # Optionally (re)create schema
    if args.run_crebas:
        run_sql_file(engine, args.crebas)

    # Write in dependency order: hubs -> links -> sats
    write_tables(engine, hubs)
    write_tables(engine, links)
    write_tables(engine, sats)

    print('ETL completed:')
    for group_name, group in [('Hubs', hubs), ('Links', links), ('Sats', sats)]:
        total = sum(len(df_) for df_ in group.values())
        print(f"  {group_name}: {total:,} rows across {len(group)} tables")


if __name__ == '__main__':
    main()
