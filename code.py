import requests
import json
from tqdm.notebook import tqdm
import pandas as pd


BRANDS_TO_PROCESS=["J","C","D","Y","X","R"]
BRAND_CODE_TO_BRAND={
    "J":"jeep",
    "C":"chrysler",
    "D":"dodge",
    "Y":"alfa",
    "R":"ram",
    "X":"fiat"
}
DATA_API=""

def format_dealer_data(dealer: dict) -> dict:
    return {
        "dealerCode": dealer.get("dealerCode"),
        "locationSeq": dealer.get("locationSeq"),
        "dealerName": dealer.get("dealerName"),
        "dealerAddress1": dealer.get("dealerAddress1"),
        "dealerAddress2": dealer.get("dealerAddress2"),
        "dealerCity": dealer.get("dealerCity"),
        "dealerState": dealer.get("dealerState"),
        "dealerShowroomCountry": dealer.get("dealerShowroomCountry"),
        "dealerZipCode": dealer.get("dealerZipCode"),
        "dealerShowroomLongitude": dealer.get("dealerShowroomLongitude"),
        "dealerShowroomLatitude": dealer.get("dealerShowroomLatitude"),
        "businessCenter": dealer.get("businessCenter"),
        "phoneNumber": dealer.get("phoneNumber"),
        "distance": dealer.get("distance"),
        "dma": dealer.get("dma"),
        "website": dealer.get("website"),
        "demail": dealer.get("demail"),
        "hasQuote": dealer.get("hasQuote"),
        "departments": str(dealer.get("departments")),
        "brands": str(dealer.get("brands")),
        "services": str(dealer.get("services")),
        "awards": str(dealer.get("awards")),
        "onlineServiceSchedulingURL": dealer.get("onlineServiceSchedulingURL"),
        "ossDealerid": dealer.get("ossDealerid"),
        "osspilotflag": dealer.get("osspilotflag"),
        "dealerStatus": dealer.get("dealerStatus"),
    }


print("Starting Extraction :-")
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Referer": "https://www.jeep.com/",
}


def extract_data(dealers_array):
    return list(map(format_dealer_data,dealers_array))
    
for brand in BRANDS_TO_PROCESS:
    url=DATA_API.format(brand)
    print(f"\tExtracting from :- {url}")
    data=requests.get(url,headers=headers,timeout=None)
    if data.status_code==200:
        try:
            data=data.json()
            extracted_data=extract_data(data['dealer'])
            df=pd.DataFrame(extracted_data)
            df.to_csv(f"{BRAND_CODE_TO_BRAND[brand]}_dealers.csv",index=False)
        except Exception as e:
            print("Error occured while extraction :-",e)
    else:
        print("Failed data extraction for :-",brand)
