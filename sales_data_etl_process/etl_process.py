"""
ETL Process for Global Bike Sales Data
Lädt Daten aus SalesData.csv in das normalisierte Data Warehouse
"""

import bonobo
import csv
from datetime import datetime
from collections import defaultdict
import psycopg2
from decimal import Decimal, InvalidOperation

# Datenbank-Konfiguration
DB_CONFIG = {
    'host': 'postgres.fbi.h-da.de',
    'database': '',     # ausfüllen
    'user': '',         # ausfüllen
    'password': '',     # wird in konsole abgefragt
    'port': 5432
}

# Globale Dictionaries für die Dimensionstabellen (um Duplikate zu vermeiden)
countries = {}
customers = {}
dates = {}
sales_orgs = {}
orders = {}
product_categories = {}
products = {}
fact_sales = []
errors = []


def extract_csv():
    """Extrahiert Daten aus der CSV-Datei"""
    with open('SalesData.csv', 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f, delimiter=';')
        for row_num, row in enumerate(reader, start=2):
            row['_row_num'] = row_num
            yield row


def validate_and_transform(row):
    """Validiert und transformiert die Daten"""
    row_num = row['_row_num']
    error_messages = []
    
    try:
        # Datenbereinigung: Währungen korrigieren
        currency_mapping = {
            '€': 'EUR',
            '$': 'USD',
            '£': 'GBP'
        }
        currency = row.get('Currency', '').strip()
        if currency in currency_mapping:
            original = currency
            row['Currency'] = currency_mapping[currency]
            error_messages.append(f"Zeile {row_num}: Währung '{original}' automatisch zu '{row['Currency']}' korrigiert")
        
        # Datenbereinigung: Ländercodes korrigieren
        country_mapping = {
            'GER': 'DE',
            'USA': 'US',
            'UK': 'GB',
            'FRA': 'FR'
        }
        country = row.get('Country', '').strip()
        if country in country_mapping:
            original = country
            row['Country'] = country_mapping[country]
            error_messages.append(f"Zeile {row_num}: Ländercode '{original}' automatisch zu '{row['Country']}' korrigiert")
        
        # Pflichtfelder prüfen
        if not row.get('OrderNumber'):
            error_messages.append(f"Zeile {row_num}: OrderNumber fehlt")
        
        if not row.get('OrderItem'):
            error_messages.append(f"Zeile {row_num}: OrderItem fehlt")
        
        if not row.get('Country'):
            error_messages.append(f"Zeile {row_num}: Country fehlt")
        
        # Datum parsen und validieren
        date_str = row.get('Date', '')
        try:
            # Format: DD.MM.YY
            date_obj = datetime.strptime(date_str, '%d.%m.%y')
            row['parsed_date'] = date_obj
        except ValueError:
            error_messages.append(f"Zeile {row_num}: Ungültiges Datumsformat '{date_str}'")
            row['parsed_date'] = None
        
        # Numerische Felder validieren und konvertieren
        numeric_fields = {
            'Customer': int,
            'OrderNumber': int,
            'SalesQuantity': int,
            'Revenue': Decimal,
            'Discount': Decimal,
            'RevenueUSD': Decimal,
            'DiscountUSD': Decimal,
            'CostsUSD': Decimal
        }
        
        for field, dtype in numeric_fields.items():
            try:
                value = row.get(field, '').strip()
                if value:
                    # Ersetze Komma durch Punkt für Dezimalzahlen
                    if dtype == Decimal:
                        value = value.replace(',', '.')
                    row[f'parsed_{field}'] = dtype(value)
                else:
                    row[f'parsed_{field}'] = None
                    if field in ['Customer', 'OrderNumber']:
                        error_messages.append(f"Zeile {row_num}: {field} fehlt oder ist leer")
            except (ValueError, InvalidOperation) as e:
                error_messages.append(f"Zeile {row_num}: {field} hat ungültigen Wert '{row.get(field)}'")
                row[f'parsed_{field}'] = None
        
        # Währung validieren
        currency = row.get('Currency', '').strip()
        if currency and len(currency) != 3:
            error_messages.append(f"Zeile {row_num}: Ungültige Währung '{currency}' (sollte 3-stelliger Code sein)")
        
        # Ländercode validieren
        country_code = row.get('Country', '').strip()
        if country_code and len(country_code) != 2:
            error_messages.append(f"Zeile {row_num}: Ungültiger Ländercode '{country_code}' (sollte 2-stellig sein)")
        
        # Wenn Fehler gefunden wurden, speichern
        if error_messages:
            errors.append({
                'row_num': row_num,
                'data': row,
                'errors': error_messages
            })
        
        yield row
        
    except Exception as e:
        errors.append({
            'row_num': row_num,
            'data': row,
            'errors': [f"Unerwarteter Fehler: {str(e)}"]
        })
        yield row


def load_dimension_tables(row):
    """Lädt Daten in die Dimensionstabellen (in Memory)"""
    
    # Country
    country_code = row.get('Country', '').strip()
    if country_code and country_code not in countries:
        countries[country_code] = {
            'countryCode': country_code,
            'countryName': None  # Wird nicht in den Quelldaten geliefert
        }
    
    # Customer
    customer_id = row.get('parsed_Customer')
    if customer_id and customer_id not in customers:
        customers[customer_id] = {
            'customerID': customer_id,
            'countryCode': country_code if country_code else None,
            'custDescr': row.get('CustDescr', '').strip() or None,
            'city': row.get('City', '').strip() or None
        }
    
    # Date
    date_obj = row.get('parsed_date')
    if date_obj:
        date_id = int(date_obj.strftime('%Y%m%d'))
        if date_id not in dates:
            dates[date_id] = {
                'dateID': date_id,
                'date': date_obj.date(),
                'year': date_obj.year,
                'month': date_obj.month,
                'day': date_obj.day
            }
    
    # SalesOrg
    sales_org_id = row.get('SalesOrg', '').strip()
    if sales_org_id and sales_org_id not in sales_orgs:
        sales_orgs[sales_org_id] = {
            'salesOrgID': sales_org_id,
            'salesOrgCode': sales_org_id  # In den Daten ist nur der Code vorhanden
        }
    
    # Order
    order_number = row.get('parsed_OrderNumber')
    if order_number and order_number not in orders:
        orders[order_number] = {
            'orderNumber': order_number,
            'salesOrgID': sales_org_id if sales_org_id else None,
            'currency': row.get('Currency', '').strip() or None,
            'revenue': row.get('parsed_Revenue'),
            'discount': row.get('parsed_Discount')
        }
    
    # ProductCategory
    prod_cat_id = row.get('ProdCat', '').strip()
    if prod_cat_id and prod_cat_id not in product_categories:
        product_categories[prod_cat_id] = {
            'prodCatID': prod_cat_id,
            'catDescr': row.get('CatDescr', '').strip() or None
        }
    
    # Product
    product_id = row.get('Product', '').strip()
    if product_id and product_id not in products:
        products[product_id] = {
            'productID': product_id,
            'prodCatID': prod_cat_id if prod_cat_id else None,
            'prodDescr': row.get('ProdDescr', '').strip() or None,
            'divisionCode': row.get('Division', '').strip() or None
        }
    
    # FactSales
    order_item = row.get('OrderItem', '').strip()
    if order_item:
        fact_sales.append({
            'orderItem': f"{order_number}-{order_item}",  # Kombinierter Key
            'productID': product_id if product_id else None,
            'customerID': customer_id,
            'orderNumber': order_number,
            'dateID': int(date_obj.strftime('%Y%m%d')) if date_obj else None,
            'salesQuantity': row.get('parsed_SalesQuantity'),
            'unitOfMeasure': row.get('UnitOfMeasure', '').strip() or None,
            'revenueUSD': row.get('parsed_RevenueUSD'),
            'discountUSD': row.get('parsed_DiscountUSD'),
            'costsUSD': row.get('parsed_CostsUSD')
        })
    
    yield row


def write_to_database():
    """Schreibt alle Daten in die PostgreSQL-Datenbank"""
    print("\n" + "="*80)
    print("DATENBANK-IMPORT STARTET")
    print("="*80)
    
    if not DB_CONFIG['password']:
        print("\nFEHLER: Passwort wurde nicht eingegeben!")
        return
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        print("\n✓ Verbindung zur Datenbank hergestellt")
        
        # Country
        print(f"\nLade {len(countries)} Länder...")
        for country in countries.values():
            cur.execute(
                'INSERT INTO Country (countryCode, countryName) VALUES (%s, %s) ON CONFLICT (countryCode) DO NOTHING',
                (country['countryCode'], country['countryName'])
            )
        print(f"✓ {len(countries)} Länder geladen")
        
        # SalesOrg
        print(f"\nLade {len(sales_orgs)} Vertriebsorganisationen...")
        for org in sales_orgs.values():
            cur.execute(
                'INSERT INTO SalesOrg (salesOrgID, salesOrgCode) VALUES (%s, %s) ON CONFLICT (salesOrgID) DO NOTHING',
                (org['salesOrgID'], org['salesOrgCode'])
            )
        print(f"✓ {len(sales_orgs)} Vertriebsorganisationen geladen")
        
        # Customer
        print(f"\nLade {len(customers)} Kunden...")
        for customer in customers.values():
            cur.execute(
                'INSERT INTO Customer (customerID, countryCode, custDescr, city) VALUES (%s, %s, %s, %s) ON CONFLICT (customerID) DO NOTHING',
                (customer['customerID'], customer['countryCode'], customer['custDescr'], customer['city'])
            )
        print(f"✓ {len(customers)} Kunden geladen")
        
        # Date
        print(f"\nLade {len(dates)} Datumswerte...")
        for date in dates.values():
            cur.execute(
                'INSERT INTO "Date" (dateID, "date", year, month, day) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (dateID) DO NOTHING',
                (date['dateID'], date['date'], date['year'], date['month'], date['day'])
            )
        print(f"✓ {len(dates)} Datumswerte geladen")
        
        # Order
        print(f"\nLade {len(orders)} Bestellungen...")
        for order in orders.values():
            cur.execute(
                'INSERT INTO "Order" (orderNumber, salesOrgID, currency, revenue, discount) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (orderNumber) DO NOTHING',
                (order['orderNumber'], order['salesOrgID'], order['currency'], order['revenue'], order['discount'])
            )
        print(f"✓ {len(orders)} Bestellungen geladen")
        
        # ProductCategory
        print(f"\nLade {len(product_categories)} Produktkategorien...")
        for cat in product_categories.values():
            cur.execute(
                'INSERT INTO ProductCategory (prodCatID, catDescr) VALUES (%s, %s) ON CONFLICT (prodCatID) DO NOTHING',
                (cat['prodCatID'], cat['catDescr'])
            )
        print(f"✓ {len(product_categories)} Produktkategorien geladen")
        
        # Product
        print(f"\nLade {len(products)} Produkte...")
        for product in products.values():
            cur.execute(
                'INSERT INTO Product (productID, prodCatID, prodDescr, divisionCode) VALUES (%s, %s, %s, %s) ON CONFLICT (productID) DO NOTHING',
                (product['productID'], product['prodCatID'], product['prodDescr'], product['divisionCode'])
            )
        print(f"✓ {len(products)} Produkte geladen")
        
        # FactSales
        print(f"\nLade {len(fact_sales)} Verkaufstransaktionen...")
        inserted = 0
        for fact in fact_sales:
            try:
                cur.execute(
                    'INSERT INTO FactSales (orderItem, productID, customerID, orderNumber, dateID, salesQuantity, unitOfMeasure, revenueUSD, discountUSD, costsUSD) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (orderItem) DO NOTHING',
                    (fact['orderItem'], fact['productID'], fact['customerID'], fact['orderNumber'], 
                     fact['dateID'], fact['salesQuantity'], fact['unitOfMeasure'], 
                     fact['revenueUSD'], fact['discountUSD'], fact['costsUSD'])
                )
                inserted += 1
            except Exception as e:
                errors.append({
                    'row_num': 'FactSales',
                    'data': fact,
                    'errors': [f"Fehler beim Einfügen: {str(e)}"]
                })
        print(f"✓ {inserted} Verkaufstransaktionen geladen")
        
        conn.commit()
        print("\n✓ Alle Daten erfolgreich in die Datenbank geschrieben!")
        print("="*80)
        
    except Exception as e:
        print(f"\n✗ FEHLER beim Datenbankzugriff: {str(e)}")
        if 'conn' in locals():
            conn.rollback()
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()


def write_error_report():
    """Schreibt einen Fehlerbericht"""
    print("\n" + "="*80)
    print("FEHLERBERICHT")
    print("="*80)
    
    if not errors:
        print("\n✓ Keine Fehler gefunden!")
    else:
        print(f"\n{len(errors)} fehlerhafte Datensätze gefunden:\n")
        
        with open('fehlerhafte_datensaetze.txt', 'w', encoding='utf-8') as f:
            f.write("FEHLERBERICHT - Global Bike Sales Data ETL\n")
            f.write("="*80 + "\n\n")
            f.write(f"Anzahl fehlerhafter Datensätze: {len(errors)}\n\n")
            
            for error in errors:
                f.write(f"\nZeile {error['row_num']}:\n")
                f.write("-" * 40 + "\n")
                for err_msg in error['errors']:
                    f.write(f"  • {err_msg}\n")
                    print(f"  • {err_msg}")
                f.write(f"  Daten: {error['data']}\n\n")
        
        print(f"\n✓ Vollständiger Fehlerbericht wurde in 'fehlerhafte_datensaetze.txt' gespeichert")
    
    print("="*80)


def get_graph(**options):
    """
    Erstellt den Bonobo ETL-Graph
    """
    graph = bonobo.Graph()
    
    graph.add_chain(
        extract_csv,
        validate_and_transform,
        load_dimension_tables,
    )
    
    return graph


def get_services(**options):
    """
    Definiert Services für den ETL-Prozess
    """
    return {}


if __name__ == '__main__':
    print("\n" + "="*80)
    print("GLOBAL BIKE SALES DATA - ETL PROCESS")
    print("="*80)
    print("\nDieser ETL-Prozess lädt die Daten aus SalesData.csv")
    print("in das normalisierte Data Warehouse.\n")
    
    # Passwort abfragen wenn nicht gesetzt
    if not DB_CONFIG['password']:
        import getpass
        DB_CONFIG['password'] = getpass.getpass("Datenbank-Passwort eingeben: ")
    
    # Bonobo ETL ausführen
    parser = bonobo.get_argument_parser()
    with bonobo.parse_args(parser) as options:
        bonobo.run(
            get_graph(**options),
            services=get_services(**options)
        )
    
    # Daten in Datenbank schreiben
    write_to_database()
    
    # Fehlerbericht erstellen
    write_error_report()
    
    # Statistik
    print("\n" + "="*80)
    print("ZUSAMMENFASSUNG")
    print("="*80)
    print(f"\nGeladene Dimensionen:")
    print(f"  • {len(countries)} Länder")
    print(f"  • {len(customers)} Kunden")
    print(f"  • {len(dates)} Datumswerte")
    print(f"  • {len(sales_orgs)} Vertriebsorganisationen")
    print(f"  • {len(orders)} Bestellungen")
    print(f"  • {len(product_categories)} Produktkategorien")
    print(f"  • {len(products)} Produkte")
    print(f"  • {len(fact_sales)} Verkaufstransaktionen")
    print(f"\nFehler: {len(errors)} fehlerhafte Datensätze")
    print("="*80 + "\n")

