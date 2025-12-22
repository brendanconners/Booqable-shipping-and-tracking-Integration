import requests
import pandas as pd

API_KEY = "7c88004786ef273d44e157c4e179d854689beb4246f9008b898f607930a29bc4"
BASE_URL_ORDERS = "https://quora-legal.booqable.com/api/4/orders"
BASE_URL_PLANNINGS = "https://quora-legal.booqable.com/api/4/plannings"
BASE_URL_ITEMS = "https://quora-legal.booqable.com/api/4/items"
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

print(f"Total orders fetched: {len(all_orders)}")

# -------------------------
# FETCH FULL PLANNINGS FOR EACH ORDER
# -------------------------
planning_rows = []

for o in all_orders:
    order_id = o.get("id")
    order_number = o.get("attributes", {}).get("number")
    customer_id = o.get("attributes", {}).get("customer_id")

    # Fetch all plannings for this order using filter
    params = {
        "filter[order_id]": order_id,
        "include": "order,item,nested_plannings,start_location,stop_location"
    }
    pl_resp = requests.get(BASE_URL_PLANNINGS, headers=HEADERS, params=params)
    if pl_resp.status_code != 200:
        print(f"Error fetching plannings for order {order_number}")
        continue

    plannings_list = pl_resp.json().get("data", [])

    # Process each planning
    for p in plannings_list:
        planning_id = p.get("id")
        attrs = p.get("attributes", {})
        rels = p.get("relationships", {})

        # Extract order_number from included order relationship
        order_data = rels.get("order", {}).get("data", {})
        order_num_included = order_data.get("attributes", {}).get("number") if order_data else order_number

        # Extract item_name from included item relationship
        item_data = rels.get("item", {}).get("data", {})
        item_name = item_data.get("attributes", {}).get("name") if item_data else None

        item_id = item_data.get("id") if item_data else None
        item_type = item_data.get("type") if item_data else None

        # Add main planning
        planning_rows.append({
            "order_number": order_num_included,
            "customer_id": customer_id,
            "planning_id": planning_id,
            "parent_planning_id": attrs.get("parent_planning_id"),
            "item_id": item_id,
            "item_type": item_type,
            "item_name": item_name,
            "quantity": attrs.get("quantity"),
        })

        # Add nested plannings if any
        nested_list = rels.get("nested_plannings", {}).get("data", [])
        for np in nested_list:
            np_id = np.get("id")
            np_attrs = np.get("attributes", {})
            np_item = np.get("relationships", {}).get("item", {}).get("data", {})

            np_item_name = np_item.get("attributes", {}).get("name") if np_item else None
            np_item_id = np_item.get("id") if np_item else None
            np_item_type = np_item.get("type") if np_item else None

            planning_rows.append({
                "order_number": order_num_included,
                "customer_id": customer_id,
                "planning_id": np_id,
                "parent_planning_id": np_attrs.get("parent_planning_id"),
                "item_id": np_item_id,
                "item_type": np_item_type,
                "item_name": np_item_name,
                "quantity": np_attrs.get("quantity"),
                "status": np_attrs.get("status"),

            })

# -------------------------
# SAVE TO CSV
# -------------------------
plannings_df = pd.DataFrame(planning_rows)
plannings_items =plannings_df['item_id']
print(plannings_items.head())
plannings_df.to_csv("practice_orders.csv", index=False)

# Item API Call 
all_items = []
while True:
    params = {
        "page[number]": page_number,
        "page[size]": page_size
    }
    response = requests.get(BASE_URL_ITEMS, headers=HEADERS, params=params)
    if response.status_code != 200:
        print(f"Error fetching items: {response.status_code}")
        print(response.text)
        break

    data = response.json()
    items = data.get("data", [])
    if not items:
        break

    all_items.extend(items)

    # Pagination
    meta = data.get("meta", {}).get("page", {})
    total_pages = meta.get("total_pages", 1)
    if page_number >= total_pages:
        break
    page_number += 1

print(f"Total items fetched: {len(all_items)}")

