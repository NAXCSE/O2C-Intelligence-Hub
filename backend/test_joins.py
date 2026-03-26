import sqlite3

conn = sqlite3.connect('data/business.db')
cur = conn.cursor()

print('=== TABLE ROW COUNTS ===')
tables = ['sales_order_headers','sales_order_items','delivery_headers','delivery_items','billing_headers','billing_items','journal_entries','payments','business_partners','products']
for t in tables:
    cur.execute(f'SELECT COUNT(*) FROM {t}')
    print(f'  {t}: {cur.fetchone()[0]} rows')

print()
print('=== JOIN TESTS ===')

print()
print('1. Sales Orders -> Delivery Items:')
cur.execute('''
    SELECT COUNT(*) FROM sales_order_headers soh
    JOIN delivery_items di ON di.referenceSdDocument = soh.salesOrder
''')
print(f'   Matched rows: {cur.fetchone()[0]}')

print()
print('2. Delivery Items -> Billing Items:')
cur.execute('''
    SELECT COUNT(*) FROM delivery_items di
    JOIN billing_items bi ON bi.referenceSdDocument = di.deliveryDocument
''')
print(f'   Matched rows: {cur.fetchone()[0]}')

print()
print('3. Billing Headers -> Journal Entries:')
cur.execute('''
    SELECT COUNT(*) FROM billing_headers bh
    JOIN journal_entries je ON je.accountingDocument = bh.accountingDocument
''')
print(f'   Matched rows: {cur.fetchone()[0]}')

print()
print('4. Sales Orders -> Business Partners:')
cur.execute('''
    SELECT COUNT(*) FROM sales_order_headers soh
    JOIN business_partners bp ON bp.businessPartner = soh.soldToParty
''')
print(f'   Matched rows: {cur.fetchone()[0]}')

print()
print('5. Sales Order Items -> Products:')
cur.execute('''
    SELECT COUNT(*) FROM sales_order_items soi
    JOIN products p ON p.product = soi.material
''')
print(f'   Matched rows: {cur.fetchone()[0]}')

print()
print('=== SAMPLE FULL FLOW ===')
cur.execute('''
    SELECT 
        soh.salesOrder,
        di.deliveryDocument,
        bi.billingDocument,
        je.accountingDocument
    FROM sales_order_headers soh
    JOIN delivery_items di ON di.referenceSdDocument = soh.salesOrder
    JOIN billing_items bi ON bi.referenceSdDocument = di.deliveryDocument
    JOIN billing_headers bh ON bh.billingDocument = bi.billingDocument
    JOIN journal_entries je ON je.accountingDocument = bh.accountingDocument
    LIMIT 3
''')
rows = cur.fetchall()
for row in rows:
    print(f'  Order:{row[0]} -> Delivery:{row[1]} -> Billing:{row[2]} -> Journal:{row[3]}')

conn.close()
print()
print('All join tests complete!')