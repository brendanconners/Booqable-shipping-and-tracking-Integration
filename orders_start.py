import requests
import pandas as pd
import ast
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
    order_id = o.get('id')
    plannings_url = o.get('relationships', {}).get('plannings', {}).get('links', {}).get('related')
    plannings = []
    
    row = {
        'order_number': attributes.get('number'),
        'customer_id': attributes.get('customer_id'),
        'item_count': attributes.get('item_count'),
        'properties': attributes.get('properties'),

    }
    rows.append(row)
    #print(rows)

    orders_df = pd.DataFrame(rows)
    orders_df.to_csv('orders_list.csv')
    print(orders_df.head())

#----------------------------------------

#Customers Work

#--------------------------------------- 

customer_url = "https://quora-legal.booqable.com/api/4/customers"

HEADERS = {
    "Authorization": f"Bearer {API_KEY}",
    "Accept": "application/json"
}

customers_list = []

while True:
    params = {
        "page[number]": page_number,
        "page[size]": page_size
    }
    response = requests.get(customer_url, headers=HEADERS, params=params)
    if response.status_code != 200:
        print(f"Error fetching customers: {response.status_code}")
        print(response.text)
        break

    data = response.json()
    customers = data.get("data", [])
    if not customers:
        break

    customers_list.extend(customers)

    # Pagination
    meta = data.get("meta", {}).get("page", {})
    total_pages = meta.get("total_pages", 1)
    if page_number >= total_pages:
        break
    page_number += 1

print(f" Total customers fetched: {len(customers_list)}")
#print(customers_list)
customers_listdf = pd.DataFrame(customers_list)

#lamda function to get the name attributes out of the orders dataframe: 

def get_name(x):
    if isinstance(x, dict):
        return x.get('name')
    if isinstance(x, str):
        return ast.literal_eval(x).get('name')
    return None
def get_email(y):
    if isinstance(y,dict):
        return y.get('email')
    if isinstance(y,str):
        return ast.literal_eval(y).get('email')
    return None

customers_listdf['name'] = customers_listdf['attributes'].apply(get_name)
customers_listdf['email'] = customers_listdf['attributes'].apply(get_email)

#print(customers_listdf)

#customers_listdf.head()

#Merging Orders and Customer List Dataframe

merged_df = orders_df.merge(
    customers_listdf,
    left_on='customer_id',
    right_on='id',
    how='inner'
)
(merged_df['customer_id'] == merged_df['id']).all()
merged_df = merged_df.drop(columns=['id','type',])


merged_df.to_csv('Merged_list.csv')
