import sqlite3

conn = sqlite3.connect('data/business.db')
cur = conn.cursor()

print('=== Tracing billing document 90504204 ===')

print()
print('1. Billing header:')
cur.execute("SELECT * FROM billing_headers WHERE billingDocument = '90504204'")
row = cur.fetchone()
print(row)

print()
print('2. Billing items:')
cur.execute("SELECT * FROM billing_items WHERE billingDocument = '90504204'")
for r in cur.fetchall():
    print(r)

print()
print('3. Delivery via billing items:')
cur.execute("""
    SELECT * FROM delivery_items 
    WHERE deliveryDocument IN (
        SELECT referenceSdDocument FROM billing_items 
        WHERE billingDocument = '90504204'
    )
""")
for r in cur.fetchall():
    print(r)

print()
print('4. Sales order via delivery:')
cur.execute("""
    SELECT * FROM sales_order_headers 
    WHERE salesOrder IN (
        SELECT referenceSdDocument FROM delivery_items 
        WHERE deliveryDocument IN (
            SELECT referenceSdDocument FROM billing_items 
            WHERE billingDocument = '90504204'
        )
    )
""")
for r in cur.fetchall():
    print(r)

print()
print('5. Journal entry:')
cur.execute("""
    SELECT * FROM journal_entries 
    WHERE accountingDocument IN (
        SELECT accountingDocument FROM billing_headers 
        WHERE billingDocument = '90504204'
    )
""")
for r in cur.fetchall():
    print(r)

print()
print('6. Payment:')
cur.execute("""
    SELECT * FROM payments 
    WHERE accountingDocument IN (
        SELECT clearingAccountingDocument FROM journal_entries 
        WHERE accountingDocument IN (
            SELECT accountingDocument FROM billing_headers 
            WHERE billingDocument = '90504204'
        )
    )
""")
for r in cur.fetchall():
    print(r)

conn.close()
print()
print('Done!')