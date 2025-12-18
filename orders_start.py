import requests
import pandas as pd

# -------------------------
# CONFIG
# -------------------------

API_KEY = "7c88004786ef273d44e157c4e179d854689beb4246f9008b898f607930a29bc4"
BASE_URL_ORDERS = "https://quora-legal.booqable.com/api/4/orders"
HEADERS = {
    "Authorization": f"Bearer {API_KEY}",
    "Accept": "application/json"
}

# -------------------------
# FETCH ALL ORDERS
# -------------------------
all_orders = []
page_number = 1
page_size = 100


while True:
    params = {
        "page[number]": page_number,
        "page[size]": page_size
    }
    response = requests.get(BASE_URL_ORDERS, headers=HEADERS, params=params)
    if response.status_code != 200:
        print(f"Error fetching Orders: {response.status_code}")
        print(response.text)
        break

    data = response.json()
    orders = data.get("data", [])
    if not orders:
        break

    all_orders.extend(orders)

    # Pagination
    meta = data.get("meta", {}).get("page", {})
    total_pages = meta.get("total_pages", 1)
    if page_number >= total_pages:
        break
    page_number += 1

print(f"✅ Total orders fetched: {len(all_orders)}")
#print(all_orders)

rows = []

for o in all_orders:
    attributes = o.get("attributes", {})
    row = {
        'order_number': o.get('number'),
        'customer_id': o.get('customer_id'),
        'item_count': o.get('item_count'),
        'properties': o.get('properties'),

    }
    rows.append(row)
    #print(rows)

    orders_df = pd.DataFrame(rows)
   # print(orders_df.head())

#----------------------------------------

#Customers Work

#--------------------------------------- 

customer_url = "https://quora-legal.booqable.com/api/4/customers"