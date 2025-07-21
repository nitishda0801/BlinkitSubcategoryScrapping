import requests
import pandas as pd
import time

INPUT_FILE = "input.csv"
OUTPUT_FILE = "output.csv"
DELAY = 1  # seconds between requests

# --- Fetch data from BlinkIt public API ---
def fetch_subcategory(lat, lon, subcat_id, page=1, page_size=100):
    url = f"https://www.blinkit.com/api/v2/product/sub-category/{int(subcat_id)}"
    params = {"lat": lat, "lon": lon, "page": page, "size": page_size}
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept": "application/json",
        "Referer": "https://blinkit.com/",
        "Origin": "https://blinkit.com",
        "x-location": f"{lat},{lon}"
    }

    try:
        resp = requests.get(url, params=params, headers=headers)
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.HTTPError as e:
        print(f"❌ HTTP Error: {e}")
        return {}
    except requests.exceptions.RequestException as e:
        print(f"❌ Request Error: {e}")
        return {}

# --- Parse the product JSON into structured records ---
def parse_products(data, lat, lon, cat_id, sub_id):
    items = []
    for prod in data.get("products", []):
        p = {
            "latitude": lat,
            "longitude": lon,
            "category_id": cat_id,
            "subcategory_id": sub_id,
            "product_id": prod.get("id"),
            "name": prod.get("name"),
            "brand": prod.get("brand", {}).get("name"),
            "price_value": prod.get("price", {}).get("value"),
            "price_mrp": prod.get("price", {}).get("mrp"),
            "price_discount": prod.get("price", {}).get("discount"),
            "pack_size": prod.get("packSize", None),
            "rating_value": prod.get("rating", {}).get("value"),
            "rating_count": prod.get("rating", {}).get("count"),
            "image_url": prod.get("images", [None])[0]
        }
        items.append(p)
    return items

# --- Main execution ---
def main():
    # --- Manual setup for demo: add a working row ---
    if not pd.io.common.file_exists(INPUT_FILE):
        print("🔧 Creating test input.csv with real subcategory ID for Nachos...")
        test_data = {
            'latitude': [12.9716],
            'longitude': [77.5946],
            'category_id': [1502],           # Snacks & Munchies
            'subcategory_id': [150203]       # Nachos
        }
        pd.DataFrame(test_data).to_csv(INPUT_FILE, index=False)

    df_input = pd.read_csv(INPUT_FILE)
    all_products = []

    for idx, row in df_input.iterrows():
        lat, lon = row.latitude, row.longitude
        cat_id, sub_id = row.category_id, row.subcategory_id
        page = 1

        while True:
            print(f"📦 Fetching SubCategory {sub_id} at {lat},{lon} - Page {page}...")
            data = fetch_subcategory(lat, lon, sub_id, page=page)

            if not data:
                print(f"⚠️ No data returned for SubCategory {sub_id}")
                break

            prods = parse_products(data, lat, lon, cat_id, sub_id)
            if not prods:
                break

            all_products.extend(prods)
            page += 1

            if not data.get("hasMorePages", False):
                break

            time.sleep(DELAY)

    # --- Save output ---
    if all_products:
        df_out = pd.DataFrame(all_products)
        df_out.to_csv(OUTPUT_FILE, index=False)
        print(f"\n✅ Done! Saved {len(df_out)} rows to {OUTPUT_FILE}")
    else:
        print("⚠️ No products found. Output file not generated.")

if __name__ == "__main__":
    main()
