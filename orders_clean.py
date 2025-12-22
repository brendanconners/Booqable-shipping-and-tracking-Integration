import requests
import pandas as pd
import ast
import time
import traceback
import Booqable_plannings as bp
import Booqable_start as bs
import orders_start as ords

# -------------------------
# CONFIG
# -------------------------
API_KEY = "7c88004786ef273d44e157c4e179d854689beb4246f9008b898f607930a29bc4"
BASE_URL_ORDERS = "https://quora-legal.booqable.com/api/4/orders"
BASE_URL_CUSTOMERS = "https://quora-legal.booqable.com/api/4/customers"
HEADERS = {
    "Authorization": f"Bearer {API_KEY}",
    "Accept": "application/json"
}
PAGE_SIZE = 100
SLEEP_SECONDS = 300  # 5 minutes


# -------------------------
# Helper Functions
# -------------------------
def fetch_all_pages(url):
    """Fetch all pages of data from the API"""
    results = []
    page_number = 1

    while True:
        params = {"page[number]": page_number, "page[size]": PAGE_SIZE}
        response = requests.get(url, headers=HEADERS, params=params)
        if response.status_code != 200:
            print(f"Error fetching {url}: {response.status_code}")
            print(response.text)
            break

        data = response.json()
        items = data.get("data", [])
        if not items:
            break

        results.extend(items)

        # Pagination
        meta = data.get("meta", {}).get("page", {})
        total_pages = meta.get("total_pages", 1)
        if page_number >= total_pages:
            break
        page_number += 1

    return results


def extract_attribute_field(x, field):
    """Extract a field from attributes (handles dict or str)"""
    if isinstance(x, dict):
        return x.get(field)
    if isinstance(x, str):
        return ast.literal_eval(x).get(field)
    return None


def extract_property(x, key):
    """Extract a nested property from attributes['properties']"""
    if isinstance(x, dict):
        return x.get("properties", {}).get(key)
    if isinstance(x, str):
        return ast.literal_eval(x).get("properties", {}).get(key)
    return None


def build_orders_df(orders_data):
    """Convert orders API data to DataFrame"""
    rows = []
    for o in orders_data:
        attributes = o.get("attributes", {})
        row = {
            "order_number": attributes.get("number"),
            "customer_id": attributes.get("customer_id"),
            "item_count": attributes.get("item_count"),
            "properties": attributes.get("properties"),
        }
        rows.append(row)
    return pd.DataFrame(rows)


def build_customers_df(customers_data):
    """Convert customers API data to DataFrame and extract fields"""
    df = pd.DataFrame(customers_data)
    df["name"] = df["attributes"].apply(lambda x: extract_attribute_field(x, "name"))
    df["email"] = df["attributes"].apply(lambda x: extract_attribute_field(x, "email"))
    df["phone"] = df["attributes"].apply(lambda x: extract_property(x, "phone"))
    return df


def merge_orders_customers(orders_df, customers_df):
    """Merge orders and customer DataFrames"""
    merged_df = orders_df.merge(
        customers_df,
        left_on="customer_id",
        right_on="id",
        how="inner"
    )
    # Drop unnecessary columns
    merged_df = merged_df.drop(columns=["id", "type", "attributes", "relationships"], errors="ignore")
    return merged_df


# -------------------------
# Continuous Loop
# -------------------------
while True:
    try:
        print("Fetching orders...")
        all_orders = fetch_all_pages(BASE_URL_ORDERS)
        print(f"Total orders fetched: {len(all_orders)}")
        orders_df = build_orders_df(all_orders)

        print("Fetching customers...")
        all_customers = fetch_all_pages(BASE_URL_CUSTOMERS)
        print(f"Total customers fetched: {len(all_customers)}")
        customers_df = build_customers_df(all_customers)

        print("Merging orders and customers...")
        merged_df = merge_orders_customers(orders_df, customers_df)

        merged_df.to_csv("Merged_list.csv", index=False)
        print("Merged CSV saved: Merged_list.csv")
        print(merged_df.head())

    except Exception as e:
        print(" Error occurred during run:")
        traceback.print_exc()

    print(f"Sleeping for {SLEEP_SECONDS} seconds...\n")
    #time.sleep(SLEEP_SECONDS)

    #Ctrl + c stops code
