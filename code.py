import requests
import json
import pandas as pd
from tqdm.notebook import tqdm

# Brand details
BRANDS_TO_PROCESS = ["J", "C", "D", "Y", "X", "R"]
BRAND_CODE_TO_BRAND = {
    "J": "jeep",
    "C": "chrysler",
    "D": "dodge",
    "Y": "alfa",
    "R": "ram",
    "X": "fiat"
}


def safe_str(value):
    return str(value) if value is not None else None

def format_dealer_data(dealer: dict, brand: str) -> dict:
    departments = dealer.get("departments", {})
    departments_filtered = {
        "sales": departments.get("sales"),
        "service": departments.get("service")
    }

    return {
        "brand": brand,
        "dealerCode": safe_str(dealer.get("dealerCode")),
        "locationSeq": safe_str(dealer.get("locationSeq")),
        "dealerName": safe_str(dealer.get("dealerName")),
        "dealerAddress1": safe_str(dealer.get("dealerAddress1")),
        "dealerAddress2": safe_str(dealer.get("dealerAddress2")),
        "dealerCity": safe_str(dealer.get("dealerCity")),
        "dealerState": safe_str(dealer.get("dealerState")),
        "dealerShowroomCountry": safe_str(dealer.get("dealerShowroomCountry")),
        "dealerZipCode": safe_str(dealer.get("dealerZipCode")),
        "dealerShowroomLongitude": safe_str(dealer.get("dealerShowroomLongitude")),
        "dealerShowroomLatitude": safe_str(dealer.get("dealerShowroomLatitude")),
        "businessCenter": safe_str(dealer.get("businessCenter")),
        "phoneNumber": safe_str(dealer.get("phoneNumber")),
        "distance": safe_str(dealer.get("distance")),
        "dma": safe_str(dealer.get("dma")),
        "website": safe_str(dealer.get("website")),
        "demail": safe_str(dealer.get("demail")),
        "hasQuote": safe_str(dealer.get("hasQuote")),
        "departments": json.dumps(departments_filtered),  # safely store dict as JSON string
        "brands": safe_str(dealer.get("brands")),
        "services": safe_str(dealer.get("services")),
        "awards": safe_str(dealer.get("awards")),
        "onlineServiceSchedulingURL": safe_str(dealer.get("onlineServiceSchedulingURL")),
        "ossDealerid": safe_str(dealer.get("ossDealerid")),
        "osspilotflag": safe_str(dealer.get("osspilotflag")),
        "dealerStatus": safe_str(dealer.get("dealerStatus")),
    }

print("Starting Extraction ...")

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Referer": "https://www.jeep.com/",
}

all_dealers = []

for brand in tqdm(BRANDS_TO_PROCESS, desc="Extracting brands"):
    url = DATA_API.format(brand)
    print(f"→ Fetching from: {url}")
    try:
        res = requests.get(url, headers=headers, timeout=None)
        if res.status_code == 200:
            data = res.json()
            dealers = data.get("dealer", [])
            formatted = [format_dealer_data(d, BRAND_CODE_TO_BRAND[brand]) for d in dealers]
            all_dealers.extend(formatted)
        else:
            print(f"Failed for {brand} with status {res.status_code}")
    except Exception as e:
        print(f"Error for {brand}: {e}")

df = pd.DataFrame(all_dealers)

csv_path = "all_brands_dealers.csv"
df.to_csv(csv_path, index=False)

print(f"Data extraction completed! Saved to {csv_path}")
