import requests
import pandas as pd
import csv
import os


# -------------------------
# CONFIG
# -------------------------
API_KEY = "7c88004786ef273d44e157c4e179d854689beb4246f9008b898f607930a29bc4"
BASE_URL_PRODUCTS = "https://quora-legal.booqable.com/api/4/products"
HEADERS = {
    "Authorization": f"Bearer {API_KEY}",
    "Accept": "application/json"
}
exported_products_filename = "Booqable/products-export-2025-11-12.csv"
# -------------------------
# FETCH ALL PRODUCTS
# -------------------------
all_products = []
page_number = 1
page_size = 100  # adjust if needed

while True:
    params = {
        "page[number]": page_number,
        "page[size]": page_size
    }
    response = requests.get(BASE_URL_PRODUCTS, headers=HEADERS, params=params)
    if response.status_code != 200:
        print(f"Error fetching products: {response.status_code}")
        print(response.text)
        break

    data = response.json()
    products = data.get("data", [])
    if not products:
        break

    all_products.extend(products)

    # Pagination
    meta = data.get("meta", {}).get("page", {})
    total_pages = meta.get("total_pages", 1)
    if page_number >= total_pages:
        break
    page_number += 1

print(f"✅ Total products fetched: {len(all_products)}")

# -------------------------
# EXTRACT ATTRIBUTES
# -------------------------
rows = []
for p in all_products:
    attributes = p.get("attributes", {})
    row = {
        "product_id": p.get("id"),
        "name": attributes.get("name"),
        "sku": attributes.get("sku"),
        "type": attributes.get("type"),
        "archived": attributes.get("archived"),
        "created_at": attributes.get("created_at"),
        "updated_at": attributes.get("updated_at"),
        "price_type": attributes.get("price_type"),
        "price_period": attributes.get("price_period"),
        "base_price_in_cents": attributes.get("base_price_in_cents"),
        "photo_url": attributes.get("photo_url"),
        "description": attributes.get("description"),
        "tag_list": attributes.get("tag_list"),
        "trackable": attributes.get("trackable"),
        "has_variations": attributes.get("has_variations"),
        "variation": attributes.get("variation"),
        "shortage_limit": attributes.get("shortage_limit")
    }
    rows.append(row)

# -------------------------
# CREATE DATAFRAME
# -------------------------
df_products = pd.DataFrame(rows)
#print(df_products.head())
print(f"✅ Total products in DataFrame: {len(df_products)}")
print(df_products.columns.tolist())
df_products.to_csv('booqable_products.csv')
df_products_id = df_products['product_id']
print(df_products_id.head())
all_barcodes = []

# Getting Barcodes and Associating them with Product ID's
barcode_url = f"https://quora-legal.booqable.com/api/4/barcodes"
while True:
    params = {
        "page[number]": page_number,
        "page[size]": page_size
    }
    response2 = requests.get(barcode_url, headers=HEADERS, params=params)
    if response2.status_code != 200:
        print(f"Error fetching products: {response2.status_code}")
        print(response2.text)
        break

    data = response2.json()
    products2 = data.get("data", [])
    if not products2:
        break

    all_barcodes.extend(products2)

    # Pagination
    meta = data.get("meta", {}).get("page", {})
    total_pages = meta.get("total_pages", 1)
    if page_number >= total_pages:
        break
    page_number += 1

print(f" Total barcodes fetched: {len(all_barcodes)}")

barcodes_df = pd.DataFrame(all_barcodes)
barcodes_df.to_csv('barcodes.dataframe.csv')

#Booqable exported Inventory (reads as Products)

inventory_export = pd.read_csv('Booqable/products-export-2025-11-12.csv')

print(inventory_export)

# Cleaning up the CSV File
"""
inventory_export.drop(['taxable', 'price_ruleset', 'tax_category','base_price','base_price_as_decimal','base_price_in_cents','deposit','deposit_as_decimal',
                       'deposit_in_cents'], axis=1, inplace=True)
print(inventory_export)

# Delelting the original File and putting the new file (cleaned Up)
f =open(exported_products_filename, "w+")
inventory_export.to_csv(exported_products_filename)
f.close
"""

