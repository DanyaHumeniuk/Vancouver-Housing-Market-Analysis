import requests           # lets the python program to talk to APIs
import pandas as pd       # lets you work with data tables
import time               # tools for measuring time or adding delays
import os                 # let's you interact with ur file system like folder, etc.

DATASET_ID = "property-tax-report"
BASE_URL = f"https://opendata.vancouver.ca/api/explore/v2.1/catalog/datasets/{DATASET_ID}/records"
MAX_ROWS_PER_CALL = 100
MAX_RECORDS = 10000

def fetch_data_from_api(offset, limit=MAX_ROWS_PER_CALL):
    """Fetches a batch of property data from the Vancouver Open Data API."""
    params = {
        "limit": limit,
        "offset": offset,
        "order_by": "folio", # Used for consistent pagination
        "select": "folio, from_civic_number, street_name, property_postal_code, neighbourhood_code, legal_type, year_built, current_land_value, current_improvement_value, tax_assessment_year"
    }

    try:
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()    # if there is an error this will send us to except block
        data = response.json()
        return data.get('results', []) # return the value under results key or an empty list
    
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data at offset {offset}: {e}")
        return None
    
if __name__ == "__main__":
    all_records = []
    current_offset = 0

    print(f"Starting API data pull for {DATASET_ID}...")

    while current_offset < MAX_RECORDS:
        batch = fetch_data_from_api(current_offset)

        if batch is None or not batch:
            print("Reached end of data or encountered an error.")
            break

        all_records.extend(batch)
        print(f"Successfully fetched {len(batch)} records. Total records collected: {len(all_records)}")

        current_offset += len(batch)
        time.sleep(1)      # Wait a minute between calls

    print(f"\nAPI Data Collection Complete. Total records: {len(all_records)}")

    if all_records:
        df_raw = pd.DataFrame(all_records)
        df_raw.to_csv("vancouver_property_data_raw.csv", index=False)
        print("Raw data saved to vancouver_property_data_raw.csv")