import requests
import pandas as pd

API_KEY = "7c88004786ef273d44e157c4e179d854689beb4246f9008b898f607930a29bc4"
BASE_URL_ORDERS = "https://quora-legal.booqable.com/api/4/orders"
BASE_URL_PLANNINGS = "https://quora-legal.booqable.com/api/4/plannings"
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

# -------------------------
# FETCH FULL PLANNINGS FOR EACH ORDER USING FILTER
# -------------------------
planning_rows = []

for o in all_orders:
    order_id = o.get("id")
    order_number = o.get("attributes", {}).get("number")
    customer_id = o.get("attributes", {}).get("customer_id")

    # Fetch all plannings for this order
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

        item = rels.get("item", {}).get("data", {})
        # Add main planning
        planning_rows.append({
            "order_number": order_number,
            "customer_id": customer_id,
            "planning_id": planning_id,
            "parent_planning_id": attrs.get("parent_planning_id"),
            "item_id": item.get("id") if item else None,
            "item_type": item.get("type") if item else None,
            "quantity": attrs.get("quantity"),
            "status": attrs.get("status"),
            "planning_type": attrs.get("planning_type"),
            "starts_at": attrs.get("starts_at"),
            "stops_at": attrs.get("stops_at"),
            "reserved_from": attrs.get("reserved_from"),
            "reserved_till": attrs.get("reserved_till"),
            "start_location": attrs.get("start_location_id"),
            "stop_location": attrs.get("stop_location_id")
        })

        # Add nested plannings if any
        nested_list = rels.get("nested_plannings", {}).get("data", [])
        for np in nested_list:
            np_id = np.get("id")
            np_attrs = np.get("attributes", {})
            np_item = np.get("relationships", {}).get("item", {}).get("data", {})
            planning_rows.append({
                "order_number": order_number,
                "customer_id": customer_id,
                "planning_id": np_id,
                "parent_planning_id": np_attrs.get("parent_planning_id"),
                "item_id": np_item.get("id") if np_item else None,
                "item_type": np_item.get("type") if np_item else None,
                "quantity": np_attrs.get("quantity"),
                "status": np_attrs.get("status"),
                "planning_type": np_attrs.get("planning_type"),
                "starts_at": np_attrs.get("starts_at"),
                "stops_at": np_attrs.get("stops_at"),
                "reserved_from": np_attrs.get("reserved_from"),
                "reserved_till": np_attrs.get("reserved_till"),
                "start_location": np_attrs.get("start_location_id"),
                "stop_location": np_attrs.get("stop_location_id")
            })

# -------------------------
# SAVE TO CSV
# -------------------------
plannings_df = pd.DataFrame(planning_rows)
plannings_df.to_csv("practice_orders.csv", index=False)
print("✅ Orders and full plannings saved to 'orders_full_plannings_filtered.csv'")
print(plannings_df.head())

