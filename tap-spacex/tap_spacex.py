# Import Libraries
import singer
import pandas as pd
import sys
import numpy as np
import requests



sys.stdout.reconfigure(encoding='utf-8')

LOGGER = singer.get_logger()

def fetch_data_from_api(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        LOGGER.info(f"🔍 Raw JSON response: {data[:2]}")  # Afficher les 2 premiers objets JSON
        return pd.DataFrame(data)
    except requests.RequestException as e:
        LOGGER.error(f"Error fetching data from {url}: {e}")
        return pd.DataFrame()
    except ValueError as e:
        LOGGER.error(f"Error decoding JSON from {url}: {e}")
        return pd.DataFrame()


def fetch_launches():
    url = "https://api.spacexdata.com/v4/launches"  # URL correcte pour les lancements

    try:
        response = requests.get(url)
        response.raise_for_status()
        response.encoding = 'utf-8'  # Assurez-vous que l'encodage est correct
        LOGGER.info(f"🚀 Launches Response status: {response.status_code}")
        LOGGER.info(f"📜 Launches Response content: {response.text[:500]}")  # Afficher seulement les 500 premiers caractères
    except requests.RequestException as e:
        LOGGER.error(f"Error fetching data from {url}: {e}")
        return

    df = fetch_data_from_api(url)

    if df.empty:
        LOGGER.warning("No launches data retrieved. The DataFrame is empty!")
        return

    # Handle missing or infinite values
    df = df.replace({np.nan: None, np.inf: None, -np.inf: None})

    # Convert 'success' column to boolean, replacing None with False
    df['success'] = df['success'].apply(lambda x: False if x is None else bool(x))

    schema = {
        "properties": {
            "id": {"type": "string"},
            "name": {"type": "string"},
            "date_utc": {"type": "string", "format": "date-time"},
            "success": {"type": "boolean"},
            "rocket": {"type": "string"},
        }
    }

    records = df.to_dict(orient="records")
    LOGGER.info(f"Fetched {len(records)} launches records.")

    singer.write_schema("launches", schema, "id")
    singer.write_records("launches", records)






def fetch_rockets():
    url = "https://api.spacexdata.com/v4/rockets"

    try:
        response = requests.get(url)
        response.raise_for_status()
        LOGGER.info(f"🚀 Response status: {response.status_code}")
        LOGGER.info(f"📜 Response content: {response.text[:500]}")  # Afficher seulement les 500 premiers caractères
    except requests.RequestException as e:
        LOGGER.error(f"Error fetching data from {url}: {e}")
        return

    df = fetch_data_from_api(url)

    if df.empty:
        LOGGER.warning("No rocket data retrieved. The DataFrame is empty!")
        return

    # Handle missing or infinite values
    df = df.replace({np.nan: None, np.inf: None, -np.inf: None})

    schema = {
        "properties": {
            "id": {"type": "string"},
            "name": {"type": "string"},
            "active": {"type": "boolean"},
        }
    }

    records = df.to_dict(orient="records")
    LOGGER.info(f"Fetched {len(records)} rocket records.")

    singer.write_schema("rockets", schema, "id")
    singer.write_records("rockets", records)



def main():
    fetch_launches()
    fetch_rockets()

if __name__ == "__main__":
    main()
