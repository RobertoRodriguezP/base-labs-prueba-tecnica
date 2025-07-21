import os
import json
import requests
import zipfile
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, inspect,text
from sqlalchemy.exc import OperationalError
import numpy as np

# Paths
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
APP_DIR = os.path.join(BASE_DIR, 'app')
DATA_DIR = os.path.join(BASE_DIR, 'data')
FROM_DIR = os.path.join(DATA_DIR, 'raw_zips')
RAW_DIR = os.path.join(DATA_DIR, 'raw_csvs')
DB_PATH = os.path.join(BASE_DIR, 'database', 'annie.db')
SQL_DIR = os.path.join(DB_PATH, "..", "sql")

RENAME_MAP = {
    "PurchasesFINAL12312016": "PurchasesDec",
    "InvoicePurchases12312016": "VendorInvoicesDec",
    "EndInvFINAL12312016": "EndInvDec",
    "BegInvFINAL12312016": "BegInvDec",
    "2017PurchasePricesDec": "PricingPurchasesDec",
    "SalesFINAL12312016.csv": "SalesDec"
}

ZIP_URLS = {
    "PurchasesFINAL12312016csv.zip": "https://www.pwc.com/us/en/careers/university_relations/data_analytics_cases_studies/PurchasesFINAL12312016csv.zip",
    "BegInvFINAL12312016csv.zip": "https://www.pwc.com/us/en/careers/university_relations/data_analytics_cases_studies/BegInvFINAL12312016csv.zip",
    "2017PurchasePricesDeccsv.zip": "https://www.pwc.com/us/en/careers/university_relations/data_analytics_cases_studies/2017PurchasePricesDeccsv.zip",
    "VendorInvoices12312016csv.zip": "https://www.pwc.com/us/en/careers/university_relations/data_analytics_cases_studies/VendorInvoices12312016csv.zip",
    "EndInvFINAL12312016csv.zip": "https://www.pwc.com/us/en/careers/university_relations/data_analytics_cases_studies/EndInvFINAL12312016csv.zip",
    "SalesFINAL12312016csv.zip": "https://www.pwc.com/us/en/careers/university_relations/data_analytics_cases_studies/SalesFINAL12312016csv.zip"
}

# Ensure folders
os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

# DB engine
engine = create_engine(f'sqlite:///{DB_PATH}')

def download_zips_if_needed():
    os.makedirs("data/raw_zips", exist_ok=True)
    for filename, url in ZIP_URLS.items():
        zip_path = os.path.join("data/raw_zips", filename)
        if not os.path.exists(zip_path):
            print(f"Downloading {filename}...")
            r = requests.get(url)
            with open(zip_path, "wb") as f:
                f.write(r.content)
        else:
            print(f"Already downloaded: {filename}")

# Step 1 – Unzip all ZIPs in raw_zips/
def unzip_all():
    download_zips_if_needed()
    os.makedirs("data/raw_csvs", exist_ok=True)
    for filename in ZIP_URLS.keys():
        zip_path = os.path.join("data/raw_zips", filename)
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall("data/raw_csvs")
                print(f"Extracted {filename}")
        except zipfile.BadZipFile:
            print(f"Error: {filename} is not a valid zip file")

# Step 2 – Check if table exists
def table_exists(engine, table_name):
    inspector = inspect(engine)
    return table_name in inspector.get_table_names()

# Step 3 – Load and transform data
def load_and_transform():
    print("Loading and transforming data...")

    # Load main tables
    try:
        purchases = pd.read_sql("SELECT * FROM VendorInvoicesDec", con=engine)
        pricing = pd.read_sql("SELECT * FROM PricingPurchasesDec", con=engine)
    except Exception as e:
        raise RuntimeError(f"Error loading tables: {e}")

    # Clean numeric columns
    def clean_numeric(df, cols):
        for col in cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        return df

    purchases = clean_numeric(purchases, ["Quantity", "Dollars", "Freight"])
    pricing = clean_numeric(pricing, ["Price", "PurchasePrice", "Volume"])

    # Drop rows with null VendorNumber or key identifiers
    purchases.dropna(subset=["VendorNumber", "VendorName", "Quantity"], inplace=True)
    pricing.dropna(subset=["VendorNumber", "VendorName", "Price", "PurchasePrice"], inplace=True)

    # Merge purchases with pricing on vendor and product
    df = pd.merge(
        purchases,
        pricing,
        on=["VendorNumber", "VendorName"],
        how="inner",
        suffixes=("", "_pricing")
    )

    # Compute revenue, cost, profit, margin
    df["revenue"] = df["Price"] * df["Quantity"]
    df["cost"] = df["PurchasePrice"] * df["Quantity"]
    df["profit"] = df["revenue"] - df["cost"]
    df["margin"] = (df["profit"] / df["revenue"]).replace([np.inf, -np.inf], np.nan) * 100

    # Drop rows with non-finite profit or margin
    df = df.dropna(subset=["profit", "margin", "revenue", "cost"])

    # Standardize and lowercase final columns
    df.rename(columns={
        "Brand": "brand",
        "Description": "description"
    }, inplace=True)

    df_out = df[[
        "VendorNumber", "VendorName", "brand", "description",
        "Quantity", "revenue", "cost", "profit", "margin"
    ]].copy()

    df_out.columns = [c.lower() for c in df_out.columns]

    print(f"Transformed {len(df_out)} rows.")
    return df_out



# Step 4 – Save to SQLite
def save_to_db(df):

    df.to_sql("sales_data", con=engine, if_exists="replace", index=False)
    print(f"Data saved to {DB_PATH}")

# Step 5 – Save last updated timestamp
def save_update_timestamp():
    ts = pd.DataFrame([{"table": "sales_data", "updated_at": datetime.now().isoformat()}])
    ts.to_sql("metadata", con=engine, if_exists="replace", index=False)
    print("Update timestamp saved.")



def save_json_for_dashboard():
    print("Saving dashboard_data.json...")

    def get_dict(query, key_col, val_col):
        try:
            df = pd.read_sql(query, con=engine)
            return dict(zip(df[key_col], df[val_col]))
        except Exception as e:
            print(f"Skipped query due to error: {key_col} - {val_col} {e}")
            return {}

    
    dashboard_data = {
        
        "top_products_profit": get_dict("SELECT    description,    SUM(profit) AS profit FROM sales_data GROUP BY description ORDER BY profit DESC LIMIT 10", "description", "profit"),
        "top_products_margin": get_dict("SELECT    description,     AVG(margin) AS margin FROM sales_data GROUP BY description ORDER BY margin DESC LIMIT 10", "description", "margin"),
        "top_brands_profit": get_dict("SELECT    description,      SUM(profit) AS profit FROM sales_data GROUP BY description ORDER BY profit DESC LIMIT 10", "description","profit"),
        "top_brands_margin": get_dict("SELECT    description,     AVG(margin) AS margin FROM sales_data GROUP BY description ORDER BY margin DESC LIMIT 10", "description", "margin"),
        "losing_products": get_dict("SELECT    vendorname,     SUM(profit) AS profit FROM sales_data WHERE profit < 0 GROUP BY vendorname ORDER BY profit ASC LIMIT 10", "vendorname", "profit"),

       
        "inventory_by_brand": get_dict(
            "SELECT * FROM c1_Prep_EndInventoryByBrand", "Description", "total_inventory_value"
        ),
        "monthly_spend_per_vendor": get_dict(
            "SELECT * FROM c1_Prep_MonthlySpendPerVendor", "year_month", "total_spent"
        ),
        "price_vs_cost": get_dict(
            "SELECT * FROM c1_Prep_PriceVsPurchaseCost", "Description", "avg_margin_dollars"
        ),
        "vendor_purchase_diversity": get_dict(
            "SELECT * FROM c1_Prep_VendorPurchaseDiversity", "VendorName", "unique_products_purchased"
        )
    }
    
    


    
    os.makedirs("app/static", exist_ok=True)
    with open("app/static/dashboard_data.json", "w", encoding="utf-8") as f:
        json.dump(dashboard_data, f, indent=2)

    print("dashboard_data.json saved.")


# Main controller
def load_or_reuse(force=True):
    print("🔍 Checking ingestion requirements...")

    unzip_all()

    # Required tables (from zipped CSVs)
    required_tables = [
        "PurchasesDec", "EndInvDec", "BegInvDec","VendorInvoicesDec","PricingPurchasesDec"
    ]

    # Step 1 — Create base tables if missing
    missing_tables = [t for t in required_tables if not table_exists(engine, t)]
    if missing_tables:
        print(f"Missing base tables: {missing_tables}. Re-ingesting CSVs...")
        ingest_all_tables()
    else:
        print("All base tables already exist.")

    # Step 2 — Load & transform if 'sales_data' missing or force=True
    if force or not table_exists(engine, "sales_data"):
        print("Transforming data and generating sales_data...")
        df = load_and_transform()
        save_to_db(df)
    else:
        print("sales_data table exists. Skipping transformation.")
        df = pd.read_sql("SELECT * FROM sales_data", con=engine)

    # Step 3 — Apply views (always overwrite or validate later)
    apply_sql_views()

    # Step 4 — Generate JSON if missing or force
    dashboard_json_path = os.path.join("static", "dashboard_data.json")
    if force or not os.path.exists(dashboard_json_path):
        print("Generating dashboard_data.json...")
        save_json_for_dashboard()
    else:
        print("dashboard_data.json already exists.")

    # Step 5 — Save timestamp
    save_update_timestamp()

    

def apply_sql_views(sql_folder=SQL_DIR):
    with engine.connect() as conn:
        for file in os.listdir(sql_folder):
            if file.endswith(".sql"):
                file_path = os.path.join(sql_folder, file)
                with open(file_path, "r", encoding="utf-8") as f:
                    sql = f.read()
                    try:
                        conn.execute(text(sql))  # ← ENVUELVE AQUÍ
                        print(f"View created: {file}")
                    except Exception as e:
                        print(f"Error creating view {file}: {e}")


def ingest_all_tables():
    found = set()
    print("Ingesting CSVs into SQLite...")
    
    for csv_file in os.listdir(RAW_DIR):
        if not csv_file.endswith(".csv"):
            continue

        base_name = os.path.splitext(csv_file)[0]
        table_name = RENAME_MAP.get(base_name)

        if not table_name:
            print(f"Skipping unknown file: {csv_file}")
            continue

        print(f"Loading {csv_file} as table '{table_name}'...")
        df = pd.read_csv(os.path.join(RAW_DIR, csv_file))
        df.to_sql(table_name, con=engine, if_exists="replace", index=False)
        found.add(table_name)

    # Warn about missing critical tables
    missing = set(RENAME_MAP.values()) - found
    if missing:
        print(f"Missing expected tables: {', '.join(missing)}")
    else:
        print("All expected tables ingested successfully.")



# Entry point
if __name__ == "__main__":
    load_or_reuse(force=False)
