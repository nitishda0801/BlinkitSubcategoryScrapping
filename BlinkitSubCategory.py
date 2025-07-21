import requests
import pandas as pd
import time
from pathlib import Path

INPUT_FILE = "input.csv"
OUTPUT_FILE = "output.csv"
DELAY = 1  # seconds between requests

def fetch_subcategory(lat, lon, subcat_id, page=1, page_size=100):
    url = f"https://www.blinkit.com/api/v2/product/sub-category/{subcat_id}"
    params = {"lat": lat, "lon": lon, "page": page, "size": page_size}
    headers = {
        "User-Agent": "Mozilla/5.0 (python)",
        "x-location": f"{lat},{lon}"
    }
    resp = requests.get(url, params=params, headers=headers)
    resp.raise_for_status()
    return resp.json()

def parse_products(data, lat, lon, cat_id, sub_id):
    items = []
    for prod in data.get("products", []):
        p = {
            "latitude": lat, "longitude": lon,
            "category_id": cat_id, "subcategory_id": sub_id,
            "product_id": prod.get("id"),
            "name": prod.get("name"),
            "brand": prod.get("brand", {}).get("name"),
            "price_value": prod.get("price", {}).get("value"),
            "price_mrp": prod.get("price", {}).get("mrp"),
            "price_discount": prod.get("price", {}).get("discount"),
            "pack_size": prod.get("packSize"),
            "rating_value": prod.get("rating", {}).get("value"),
            "rating_count": prod.get("rating", {}).get("count"),
            "image_url": prod.get("images", [None])[0]
        }
        items.append(p)
    return items

def main():
    df_input = pd.read_csv(INPUT_FILE)
    all_products = []

    for idx, row in df_input.iterrows():
        lat, lon = row.latitude, row.longitude
        cat_id, sub_id = row.category_id, row.subcategory_id
        page = 1

        while True:
            print(f"Fetching subcat {sub_id} @ {lat},{lon}, page {page}…")
            data = fetch_subcategory(lat, lon, sub_id, page=page)
            prods = parse_products(data, lat, lon, cat_id, sub_id)
            if not prods:
                break
            all_products.extend(prods)
            page += 1
            if not data.get("hasMorePages", False):
                break
            time.sleep(DELAY)

    df_out = pd.DataFrame(all_products)
    df_out.to_csv(OUTPUT_FILE, index=False)
    print(f"Done! Saved {len(df_out)} rows to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
