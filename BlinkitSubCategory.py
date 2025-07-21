import requests
import pandas as pd
import time

def fetch_blinkit_products(lat, lon, subcat_id):
    url = f"https://www.blinkit.com/api/v2/product/sub-category/{subcat_id}?lat={lat}&lon={lon}"
    headers = {
        "User-Agent": "Mozilla/5.0",
        "x-location": f"{lat},{lon}"
    }
    response = requests.get(url, headers=headers)
    data = response.json()

    products = []
    for item in data.get("products", []):
        products.append({
            "name": item.get("name"),
            "brand": item.get("brand"),
            "price": item.get("price", {}).get("value"),
            "mrp": item.get("price", {}).get("mrp"),
            "discount": item.get("price", {}).get("discount"),
            "pack_size": item.get("pack_size"),
            "image_url": item.get("image_url"),
            "rating": item.get("rating", {}).get("value")
        })
    return products

# Example usage
lat, lon, subcat_id = 28.4595, 77.0266, "12345"
products = fetch_blinkit_products(lat, lon, subcat_id)
df = pd.DataFrame(products)
df.to_csv("output.csv", index=False)
