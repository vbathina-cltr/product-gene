import requests
from bs4 import BeautifulSoup
import json
import time

# Configuration
SITEMAP_URL = "https://www.wholefoodsmarket.com/sitemap/sitemap-products.xml"
API_BASE = "https://www.wholefoodsmarket.com/api/products/category/"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json"
}

def get_category_slugs():
    print("Fetching sitemap...")
    response = requests.get(SITEMAP_URL, headers=HEADERS)
    soup = BeautifulSoup(response.content, "xml")
    # Extracting the last part of the URL as the category 'slug'
    urls = [loc.text for loc in soup.find_all("loc")]
    # Filter for product category pages
    categories = [url.split('/')[-1] for url in urls if "/products/" in url]
    return list(set(categories))

def scrape_category(slug):
    all_products = []
    offset = 0
    limit = 60 # Whole Foods default page size

    while True:
        # The internal API endpoint used by the "Load More" button
        api_url = f"{API_BASE}{slug}?leafCategory={slug}&limit={limit}&offset={offset}"
        print(f"  Scraping {slug} (Offset: {offset})...")
        
        try:
            response = requests.get(api_url, headers=HEADERS)
            if response.status_code != 200:
                break
            
            data = response.json()
            products = data.get("results", [])
            
            if not products:
                break
            
            # Print each product's full data to show progress
            for product in products:
                print(f"    - Found: {json.dumps(product, indent=4)}")

            all_products.extend(products)
            offset += limit # Move to the next "page"
            time.sleep(1) # Be polite to the server
            
        except Exception as e:
            print(f"Error: {e}")
            break
            
    return all_products

# Main Execution
if __name__ == "__main__":
    slugs = get_category_slugs()
    full_database = {}

    for slug in slugs[:5]: # Testing with first 5 categories
        full_database[slug] = scrape_category(slug)

    # Save to your product folder for BigQuery ingestion
    with open("wholefoods_products.json", "w") as f:
        json.dump(full_database, f, indent=4)
    
    print(f"Success! Saved data for {len(full_database)} categories.")