#!/usr/bin/env python3
import os
import requests
import pandas as pd
from datetime import timezone, timedelta
import asyncio
import os
import libsql


def load_secret(name: str, fallback_file: str) -> str:
    """Prefer env var, else read from file, else raise."""
    value = os.getenv(name, "").strip()
    if not value and fallback_file:
        try:
            with open(fallback_file, "r", encoding="utf-8") as file:
                value = file.read().strip()
        except FileNotFoundError:
            value = ""
    if not value:
        raise RuntimeError(f"Missing required secret: {name} (env or {fallback_file})")
    return value

FUEL_API_TOKEN = load_secret("FUEL_API_TOKEN", "fuel_api_token.txt")
TURSO_TOKEN    = load_secret("TURSO_AUTH_TOKEN", "turso_token.txt")
TURSO_DB_URL   = "libsql://cairns-fuel-h-unter.aws-ap-northeast-1.turso.io"

COUNTRY_ID = 21           # Australia
CAIRNS_REGION_ID = 16     # pre-known value for Cairns (level 2)
CAIRNS_TIME_OFFSET = timedelta(hours=10)  # Queensland has no DST

FUEL_API_SUBSCRIBER_URL = "https://fppdirectapi-prod.fuelpricesqld.com.au/Subscriber"
FUEL_API_PRICE_URL      = "https://fppdirectapi-prod.fuelpricesqld.com.au/Price"

HEADERS = {
    "Authorization": f"FPDAPI SubscriberToken={FUEL_API_TOKEN}",
    "Content-Type": "application/json",
}

# Price in API is tenths-of-cents per L. Convert to $/L.
PRICE_DIVISOR = 1000.0


def fetch_brands_data() -> pd.DataFrame:
    """Fetch fuel brands from API and return the response as DataFrame"""
    request = requests.get(f"{FUEL_API_SUBSCRIBER_URL}/GetCountryBrands",
                     headers=HEADERS, params={"countryId": COUNTRY_ID})
    request.raise_for_status()
    brands = request.json()['Brands']
    return pd.DataFrame(brands)

def fetch_fuel_types_data() -> pd.DataFrame:
    """Fetch fuel types from API and return the response as DataFrame"""
    request = requests.get(f"{FUEL_API_SUBSCRIBER_URL}/GetCountryFuelTypes",
                     headers=HEADERS, params={"countryId": COUNTRY_ID})
    request.raise_for_status()
    fuels = request.json()['Fuels']
    return pd.DataFrame(fuels)

def fetch_site_data(cairns_region_id=CAIRNS_REGION_ID) -> pd.DataFrame:
    """Fetch site details for all sites in a region and return as DataFrame"""
    request = requests.get(f"{FUEL_API_SUBSCRIBER_URL}/GetFullSiteDetails",
                     headers=HEADERS,
                     params={"countryId": COUNTRY_ID, "geoRegionLevel": 2, "geoRegionId": cairns_region_id})
    request.raise_for_status()
    site_details = request.json()["S"]
    df = pd.DataFrame(site_details)
    original_name_to_sensical_name = {
        "S": "Site_ID",
        "B": "Brand_ID",
        "N": "Name",
        "A": "Address",
        "P": "Postcode",
        "Lat": "Latitude",
        "Lng": "Longitude",
    }
    df = df.rename(columns=original_name_to_sensical_name)
    return df[list(original_name_to_sensical_name.values())]

def fetch_current_price_data(cairns_region_id=CAIRNS_REGION_ID) -> pd.DataFrame:
    """Fetch current prices for all sites in a region and return as DataFrame"""
    request = requests.get(f"{FUEL_API_PRICE_URL}/GetSitesPrices",
                     headers=HEADERS,
                     params={"countryId": COUNTRY_ID, "geoRegionLevel": 2, "geoRegionId": cairns_region_id})
    request.raise_for_status()
    site_prices = request.json()["SitePrices"]
    df = pd.DataFrame(site_prices)
    rename_map = {
        "SiteId": "Site_ID",
        "FuelId": "Fuel_ID",
        "TransactionDateUtc": "TransactionDate",
        "Price": "PriceRaw"
    }
    df = df.rename(columns=rename_map)[list(rename_map.values())]
    # Parse API timestamp as UTC, then convert to local AEST (+10:00)
    
    ts = pd.to_datetime(df["TransactionDate"], utc=True, errors="coerce")
    local_ts = ts + CAIRNS_TIME_OFFSET
    # Store as ISO8601 with explicit +10:00 offset
    df["TransactionDate"] = local_ts.dt.strftime("%Y-%m-%dT%H:%M:%S+10:00")
    # Convert tenths-of-cents per L → $/L
    df["Price"] = df["PriceRaw"].astype(float) / PRICE_DIVISOR
    df = df.drop(columns=["PriceRaw"])
    # Deduplicate in this run
    df = df.dropna(subset=["Site_ID", "Fuel_ID", "TransactionDate", "Price"]).drop_duplicates(
        subset=["Site_ID", "Fuel_ID", "TransactionDate"], keep="last"
    )
    return df


def upsert_brands(cur, brands_df: pd.DataFrame):
    if brands_df.empty: return
    for brand_id, name in brands_df[["BrandId", "Name"]].dropna().drop_duplicates().itertuples(index=False):
        cur.execute(
            "INSERT INTO Brands (Brand_ID, Name) VALUES (?, ?) "
            "ON CONFLICT(Brand_ID) DO UPDATE SET Name=excluded.Name;",
            (int(brand_id), str(name))
        )

def upsert_fuel_types(cur, fuels_df: pd.DataFrame):
    if fuels_df.empty: return
    for fuel_id, name in fuels_df[["FuelId", "Name"]].dropna().drop_duplicates().itertuples(index=False):
        cur.execute(
            "INSERT INTO Fuel_Types (Fuel_ID, Name) VALUES (?, ?) "
            "ON CONFLICT(Fuel_ID) DO UPDATE SET Name=excluded.Name;",
            (int(fuel_id), str(name))
        )

def upsert_sites(cur, sites_df: pd.DataFrame):
    if sites_df.empty: return
    cols = ["Site_ID", "Brand_ID", "Name", "Address", "Postcode", "Latitude", "Longitude"]
    df = sites_df[cols].copy()
    df["Brand_ID"] = df["Brand_ID"].where(pd.notna(df["Brand_ID"]), None)

    CHUNK = 1000
    rows = [tuple(None if pd.isna(v) else v for v in r)
            for r in df.itertuples(index=False, name=None)]
    for i in range(0, len(rows), CHUNK):
        chunk = rows[i:i+CHUNK]
        placeholders = ",".join(["(?, ?, ?, ?, ?, ?, ?)"] * len(chunk))
        flat = [v for tup in chunk for v in tup]
        cur.execute(
            f"""
            INSERT INTO Sites (Site_ID, Brand_ID, Name, Address, Postcode, Latitude, Longitude)
            VALUES {placeholders}
            ON CONFLICT(Site_ID) DO UPDATE SET
              Brand_ID=COALESCE(excluded.Brand_ID, Sites.Brand_ID),
              Name=COALESCE(excluded.Name, Sites.Name),
              Address=COALESCE(excluded.Address, Sites.Address),
              Postcode=COALESCE(excluded.Postcode, Sites.Postcode),
              Latitude=COALESCE(excluded.Latitude, Sites.Latitude),
              Longitude=COALESCE(excluded.Longitude, Sites.Longitude);
            """,
            flat
        )

def upsert_prices(cur, prices_df: pd.DataFrame):
    if prices_df.empty: return
    cols = ["Site_ID", "Fuel_ID", "TransactionDate", "Price"]
    df = prices_df[cols].copy()

    CHUNK = 2000
    rows = [tuple(r) for r in df.itertuples(index=False, name=None)]
    for i in range(0, len(rows), CHUNK):
        chunk = rows[i:i+CHUNK]
        placeholders = ",".join(["(?, ?, ?, ?)"] * len(chunk))
        flat = [v for tup in chunk for v in tup]
        cur.execute(
            f"""
            INSERT INTO Price_Records (Site_ID, Fuel_ID, TransactionDate, Price)
            VALUES {placeholders}
            ON CONFLICT(Site_ID, Fuel_ID, TransactionDate)
            DO UPDATE SET Price=excluded.Price;
            """,
            flat
        )


def main():
    # fetch data from API
    brands_df = fetch_brands_data()
    fuels_df  = fetch_fuel_types_data()
    sites_df  = fetch_site_data()
    prices_df = fetch_current_price_data()

    # connect to DB and upsert
    connection = libsql.connect(
        "turso_cache.db",
        sync_url=TURSO_DB_URL,
        auth_token=TURSO_TOKEN,
        # sync_interval=60,  # optional: auto pull every N seconds
    )
    cursor = connection.cursor()
    cursor.execute("PRAGMA foreign_keys=ON;")

    upsert_brands(cursor, brands_df)
    upsert_fuel_types(cursor, fuels_df)
    upsert_sites(cursor, sites_df)
    upsert_prices(cursor, prices_df)

    connection.commit()   # push writes to primary
    connection.sync()     # pull any remote changes (optional for one-off runs)
    print(f"Upserted: brands={len(brands_df)} fuels={len(fuels_df)} "
          f"sites={len(sites_df)} prices={len(prices_df)}")

if __name__ == "__main__":
    main()