# sync_tally.py

import sys
import requests
import mysql.connector
import xml.etree.ElementTree as ET
from datetime import datetime
import json
import os

# --------------- CONFIGURATION ----------------

TABLE_CONFIG = {
    "sales": {
        "tally_collection": "Vouchers",
        "tally_xml_tag": "VOUCHER",
        "field_map": {
            "DATE": "sale_date",
            "VOUCHERTYPENAME": "voucher_type",
            "VOUCHERNUMBER": "voucher_no",
            "PARTYLEDGERNAME": "customer_name"
        },
        "sql_schema": """
            CREATE TABLE IF NOT EXISTS sales (
                sale_date VARCHAR(255),
                voucher_type VARCHAR(255),
                voucher_no VARCHAR(255),
                customer_name VARCHAR(255)
            )
        """
    },
    "ledgers": {
        "tally_collection": "Ledger",
        "tally_xml_tag": "LEDGER",
        "field_map": {
            "NAME": "ledger_name",
            "PARENT": "group_name",
            "OPENINGBALANCE": "opening_balance"
        },
        "sql_schema": """
            CREATE TABLE IF NOT EXISTS ledgers (
                ledger_name VARCHAR(255),
                group_name VARCHAR(255),
                opening_balance VARCHAR(255)
            )
        """
    }
}

LOG_FILE = "sync.log"

# --------------- UTILITY FUNCTIONS ----------------

def log(message, is_error=False):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    full_msg = f"[{timestamp}] {'ERROR' if is_error else 'INFO'}: {message}"
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(full_msg + "\n")
    print(full_msg, file=sys.stderr if is_error else sys.stdout, flush=True)

def generate_tally_request(collection):
    return f"""
    <ENVELOPE>
        <HEADER>
            <VERSION>1</VERSION>
            <TALLYREQUEST>Export</TALLYREQUEST>
            <TYPE>Collection</TYPE>
            <ID>{collection}</ID>
        </HEADER>
        <BODY>
            <DESC>
                <STATICVARIABLES>
                    <SVEXPORTFORMAT>XML</SVEXPORTFORMAT>
                </STATICVARIABLES>
            </DESC>
        </BODY>
    </ENVELOPE>
    """

def fetch_data(tally_url, collection, tag, fields):
    log(f"🔄 Fetching data from Tally for collection: {collection}")
    try:
        headers = {"Content-Type": "text/xml"}
        response = requests.post(tally_url, data=generate_tally_request(collection).encode(), headers=headers)
        response.raise_for_status()
        root = ET.fromstring(response.text)
        
        records = []
        for node in root.findall(f".//{tag}"):
            record = {field: node.find(field).text if node.find(field) is not None else None for field in fields}
            records.append(record)
        
        log(f"✅ Fetched {len(records)} records from Tally collection: {collection}")
        return records
    except Exception as e:
        log(f"❌ Tally fetch failed: {e}", is_error=True)
        return []

def ensure_table_exists(cursor, table_schema):
    cursor.execute(table_schema)

def insert_records(conn, table_name, field_map, records):
    if not records:
        log(f"ℹ️ No records to insert into table: {table_name}")
        return

    cursor = conn.cursor()
    ensure_table_exists(cursor, TABLE_CONFIG[table_name]["sql_schema"])

    db_fields = list(field_map.values())
    tally_fields = list(field_map.keys())

    placeholders = ', '.join(['%s'] * len(db_fields))
    columns = ', '.join(f"`{f}`" for f in db_fields)
    sql = f"INSERT INTO `{table_name}` ({columns}) VALUES ({placeholders})"

    values = []
    for r in records:
        row = [r.get(f, None) for f in tally_fields]
        values.append(row)

    try:
        cursor.executemany(sql, values)
        conn.commit()
        log(f"✅ Inserted {cursor.rowcount} rows into {table_name}")
    except Exception as e:
        log(f"❌ Insert failed for {table_name}: {e}", is_error=True)
    finally:
        cursor.close()

# --------------- MAIN ----------------

def main():
    if len(sys.argv) != 4:
        print("Usage: python sync_tally.py <tally_url> <db_config_json> <table1,table2,...>", file=sys.stderr)
        sys.exit(1)

    tally_url = sys.argv[1]
    db_config = json.loads(sys.argv[2])
    tables = sys.argv[3].split(',')

    log("🚀 Starting Tally Sync...")

    try:
        conn = mysql.connector.connect(**db_config)
    except Exception as e:
        log(f"❌ Could not connect to MySQL: {e}", is_error=True)
        sys.exit(1)

    for table in tables:
        table = table.strip().lower()
        if table not in TABLE_CONFIG:
            log(f"⚠️ Table '{table}' not configured. Skipping...", is_error=True)
            continue

        config = TABLE_CONFIG[table]
        field_map = config["field_map"]
        collection = config["tally_collection"]
        tag = config["tally_xml_tag"]

        records = fetch_data(tally_url, collection, tag, field_map.keys())
        insert_records(conn, table, field_map, records)

    conn.close()
    log("🏁 Sync completed successfully.")

if __name__ == "__main__":
    main()
