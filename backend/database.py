import json
import os
import sqlite3
import glob

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "data", "business.db")

TABLE_MAP = {
    "sales_order_headers": "sap-o2c-data/sales_order_headers",
    "sales_order_items": "sap-o2c-data/sales_order_items",
    "sales_order_schedule_lines": "sap-o2c-data/sales_order_schedule_lines",
    "delivery_headers": "sap-o2c-data/outbound_delivery_headers",
    "delivery_items": "sap-o2c-data/outbound_delivery_items",
    "billing_headers": "sap-o2c-data/billing_document_headers",
    "billing_items": "sap-o2c-data/billing_document_items",
    "billing_cancellations": "sap-o2c-data/billing_document_cancellations",
    "journal_entries": "sap-o2c-data/journal_entry_items_accounts_receivable",
    "payments": "sap-o2c-data/payments_accounts_receivable",
    "business_partners": "sap-o2c-data/business_partners",
    "bp_addresses": "sap-o2c-data/business_partner_addresses",
    "customer_company": "sap-o2c-data/customer_company_assignments",
    "customer_sales_area": "sap-o2c-data/customer_sales_area_assignments",
    "products": "sap-o2c-data/products",
    "product_descriptions": "sap-o2c-data/product_descriptions",
    "product_plants": "sap-o2c-data/product_plants",
    "product_storage_locations": "sap-o2c-data/product_storage_locations",
    "plants": "sap-o2c-data/plants",
}

def load_jsonl_folder(folder_path):
    records = []
    for filepath in glob.glob(f"{folder_path}/*.jsonl"):
        with open(filepath, "r") as f:
            for line in f:
                line = line.strip()
                if line:
                    records.append(json.loads(line))
    return records

def create_table_from_records(conn, table_name, records):
    if not records:
        print(f"  WARNING: No records for {table_name}")
        return

    # Get all unique keys across records
    columns = list(records[0].keys())

    cols_def = ", ".join([f'"{col}" TEXT' for col in columns])
    conn.execute(f'DROP TABLE IF EXISTS "{table_name}"')
    conn.execute(f'CREATE TABLE "{table_name}" ({cols_def})')

    for record in records:
        values = [str(record.get(col, "")) for col in columns]
        placeholders = ", ".join(["?" for _ in columns])
        conn.execute(
            f'INSERT INTO "{table_name}" VALUES ({placeholders})', values
        )

    print(f"  Loaded {len(records)} records into {table_name}")

def load_all_data(dataset_root="D:/sap-order-to-cash-dataset"):
    os.makedirs("data", exist_ok=True)
    conn = sqlite3.connect(DB_PATH)

    for table_name, relative_path in TABLE_MAP.items():
        folder = os.path.join(dataset_root, relative_path)
        print(f"Loading {table_name} from {folder}...")
        records = load_jsonl_folder(folder)
        create_table_from_records(conn, table_name, records)

    conn.commit()
    conn.close()
    print("\nAll data loaded successfully!")

if __name__ == "__main__":
    load_all_data()