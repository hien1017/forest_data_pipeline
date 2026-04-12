"""
Forest Monitoring Ingestion Pipeline
=====================================
Ingests data from three independent sources into separate BigQuery tables:
  1. Google Earth Engine (GEE)     -> forest change data
  2. Global Forest Watch (GFW)     -> deforestation alerts
  3. NASA Earthdata                -> vegetation / biomass data

Each pipeline runs independently. Failure in one does not affect the others.
Failed pipelines trigger an email alert via Gmail.
"""

import os
import time
import smtplib
import requests
import pandas as pd
from datetime import datetime, timezone
from email.mime.text import MIMEText
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account

load_dotenv()

# ---------------------------------------------------------------------------
# Shared Configuration
# ---------------------------------------------------------------------------

PROJECT_ID                = os.getenv("PROJECT_ID")
DATASET_ID                = os.getenv("DATASET_ID")
CREDENTIALS               = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
GEE_SERVICE_ACCOUNT_EMAIL = os.getenv("GEE_SERVICE_ACCOUNT_EMAIL")
GFW_API_KEY               = os.getenv("GFW_API_KEY")

MAX_RETRIES  = 3
RETRY_DELAY  = 5  # seconds between retry attempts

# ---------------------------------------------------------------------------
# Area of Interest and Date Range
# ---------------------------------------------------------------------------

# Draw your own polygon at https://geojson.io and paste the coordinates here.
# This example covers a region in Sumatra, Indonesia.
AREA_OF_INTEREST = {
    "type": "Polygon",
    "coordinates": [[
        [103.197, 0.553],
        [103.248, 0.564],
        [103.212, 0.593],
        [103.197, 0.553]
    ]]
}

# Fetch data from this date onward (YYYY-MM-DD) for gfw_deforestation_alerts only
DATA_FROM_DATE = "2020-01-01"


# ---------------------------------------------------------------------------
# Shared Helpers
# ---------------------------------------------------------------------------

def get_bigquery_client() -> bigquery.Client:
    credentials = service_account.Credentials.from_service_account_file(CREDENTIALS)
    client = bigquery.Client(project=PROJECT_ID, credentials=credentials)
    client.create_dataset(f"{PROJECT_ID}.{DATASET_ID}", exists_ok=True)
    return client


def load_dataframe_to_bigquery(
    df: pd.DataFrame,
    table_id: str,
    schema: list,
    write_disposition: str = "WRITE_TRUNCATE"
) -> None:
    if df.empty:
        print(f"  No rows to load into {table_id}. Skipping.")
        return
    client = get_bigquery_client()
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{table_id}"
    job = client.load_table_from_dataframe(
        df, table_ref,
        job_config=bigquery.LoadJobConfig(
            schema=schema,
            write_disposition=write_disposition,
        )
    )
    job.result()
    rows = client.get_table(table_ref).num_rows
    print(f"  Loaded {rows} rows into {table_ref}")


def send_failure_alert(source_name: str, error: Exception) -> None:
    """Send an email alert when a pipeline fails."""
    subject = f"Ingestion Failed: {source_name}"
    body = (
        f"The ingestion job for '{source_name}' failed.\n\n"
        f"Error type : {type(error).__name__}\n"
        f"Error detail: {error}\n\n"
        f"Timestamp: {datetime.now(timezone.utc).isoformat()}"
    )

    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"]    = os.getenv("ALERT_FROM")
    msg["To"]      = os.getenv("ALERT_TO")

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(os.getenv("ALERT_FROM"), os.getenv("GMAIL_APP_PASSWORD"))
            server.send_message(msg)
        print(f"  Alert email sent for: {source_name}")
    except Exception as mail_error:
        print(f"  Failed to send alert email: {mail_error}")


def retry_request(fn, source_name: str, step_name: str):
    """
    Call fn() up to MAX_RETRIES times. Returns the result on success.
    Raises RuntimeError after all retries are exhausted.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return fn()
        except Exception as e:
            print(f"  [{source_name}] {step_name} - attempt {attempt}/{MAX_RETRIES} failed: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)
            else:
                raise RuntimeError(
                    f"[{source_name}] {step_name} failed after {MAX_RETRIES} attempts."
                ) from e


# ===========================================================================
# Pipeline 1: Google Earth Engine (GEE) -> Forest Change
# ===========================================================================
# Requires: GEE Python SDK authenticated (earthengine-api)
# Install:  pip install earthengine-api
# Auth:     service account via GEE_SERVICE_ACCOUNT_EMAIL + GOOGLE_APPLICATION_CREDENTIALS
# Table:    gee_forest_change
# ---------------------------------------------------------------------------

GEE_TABLE_ID = "gee_forest_change"

GEE_SCHEMA = [
    bigquery.SchemaField("latitude",            "STRING"),
    bigquery.SchemaField("longitude",           "STRING"),
    bigquery.SchemaField("year",                "STRING"),
    bigquery.SchemaField("loss_area_ha",        "STRING"),
    bigquery.SchemaField("gain_area_ha",        "STRING"),
    bigquery.SchemaField("tree_cover_pct",      "STRING"),
    bigquery.SchemaField("ingestion_timestamp", "TIMESTAMP"),
]


def _gee_fetch_forest_change(geometry: dict) -> list[dict]:
    """
    Query GEE for Hansen Global Forest Change data within the given geometry.
    Samples a grid of points inside the polygon and extracts per-pixel values.
    """
    import ee  # imported here so a missing GEE install does not block other pipelines

    credentials = ee.ServiceAccountCredentials(
        email=GEE_SERVICE_ACCOUNT_EMAIL,
        key_file=CREDENTIALS,
    )
    ee.Initialize(credentials)

    # Updated to 2024 dataset (covers forest change 2000-2024)
    hansen  = ee.Image("UMD/hansen/global_forest_change_2024_v1_12")
    region  = ee.Geometry(geometry)

    samples = hansen.select(
        ["lossyear", "gain", "treecover2000"]
    ).sample(
        region=region,
        scale=100,
        geometries=True,
        numPixels=5000,
    )

    features = samples.getInfo()["features"]
    records  = []
    for feat in features:
        props     = feat["properties"]
        coords    = feat["geometry"]["coordinates"]
        loss_year = props.get("lossyear", 0)
        # Hansen is a cumulative dataset (2000-2024); include all pixels regardless of year
        records.append({
            "longitude":      coords[0],
            "latitude":       coords[1],
            "year":           (2000 + loss_year) if loss_year else None,
            "loss_area_ha":   1.0 if loss_year > 0 else 0.0,  # 1 pixel ~ 1 ha at 100 m
            "gain_area_ha":   1.0 if props.get("gain", 0) == 1 else 0.0,
            "tree_cover_pct": props.get("treecover2000"),
        })
    return records


def _gee_parse(raw: list[dict]) -> pd.DataFrame:
    now = datetime.now(timezone.utc)
    df  = pd.DataFrame(raw)
    if df.empty:
        return df
    df["ingestion_timestamp"] = now
    for col in ["latitude", "longitude", "year", "loss_area_ha", "gain_area_ha", "tree_cover_pct"]:
        df[col] = df[col].apply(lambda x: None if pd.isnull(x) else str(x))
    return df


def run_gee_pipeline() -> None:
    source = "Google Earth Engine (GEE)"
    print(f"\n[{source}] Starting...")
    try:
        raw = retry_request(
            lambda: _gee_fetch_forest_change(AREA_OF_INTEREST),
            source, "fetch"
        )
        print(f"  Fetched {len(raw)} GEE records.")

        df = _gee_parse(raw)
        print(f"  Parsed {len(df)} rows.")

        load_dataframe_to_bigquery(df, GEE_TABLE_ID, GEE_SCHEMA)
        print(f"[{source}] Done.")

    except Exception as e:
        print(f"[{source}] FAILED: {e}")
        send_failure_alert(source, e)


# ===========================================================================
# Pipeline 2: Global Forest Watch (GFW) -> Deforestation Alerts
# ===========================================================================
# Requires: GFW API key from data-api.globalforestwatch.org
# Table:    gfw_deforestation_alerts
# ---------------------------------------------------------------------------

GFW_TABLE_ID = "gfw_deforestation_alerts"
GFW_BASE_URL = "https://data-api.globalforestwatch.org"
GFW_DATASET  = "gfw_integrated_alerts"

GFW_SCHEMA = [
    bigquery.SchemaField("latitude",            "STRING"),
    bigquery.SchemaField("longitude",           "STRING"),
    bigquery.SchemaField("alert_date",          "STRING"),
    bigquery.SchemaField("intensity",           "STRING"),
    bigquery.SchemaField("confidence",          "STRING"),
    bigquery.SchemaField("ingestion_timestamp", "TIMESTAMP"),
]


def _gfw_get_latest_version() -> str:
    resp = requests.get(
        f"{GFW_BASE_URL}/dataset/{GFW_DATASET}",
        headers={"x-api-key": GFW_API_KEY},
        timeout=30,
    )
    resp.raise_for_status()
    versions = resp.json()["data"]["versions"]
    return sorted(versions)[-1]


def _gfw_query_alerts(version: str, from_date: str, geometry: dict) -> list[dict]:
    sql = (
        "SELECT longitude, latitude, "
        "gfw_integrated_alerts__date, "
        "gfw_integrated_alerts__intensity, "
        "gfw_integrated_alerts__confidence "
        f"FROM results "
        f"WHERE gfw_integrated_alerts__date >= '{from_date}'"
    )
    resp = requests.post(
        f"{GFW_BASE_URL}/dataset/{GFW_DATASET}/{version}/query/json",
        json={"sql": sql, "geometry": geometry},
        headers={"x-api-key": GFW_API_KEY, "Content-Type": "application/json"},
        timeout=60,
    )
    resp.raise_for_status()
    return resp.json().get("data", [])


def _gfw_parse(raw: list[dict]) -> pd.DataFrame:
    now  = datetime.now(timezone.utc)
    rows = [
        {
            "latitude":            str(r["latitude"]) if r.get("latitude") is not None else None,
            "longitude":           str(r["longitude"]) if r.get("longitude") is not None else None,
            "alert_date":          str(r.get("gfw_integrated_alerts__date", "")),
            "intensity":           str(r["gfw_integrated_alerts__intensity"]) if r.get("gfw_integrated_alerts__intensity") is not None else None,
            "confidence":          str(r.get("gfw_integrated_alerts__confidence", "")),
            "ingestion_timestamp": now,
        }
        for r in raw
    ]
    return pd.DataFrame(rows)


def run_gfw_pipeline() -> None:
    source = "Global Forest Watch (GFW)"
    print(f"\n[{source}] Starting...")
    try:
        version = retry_request(_gfw_get_latest_version, source, "version lookup")
        print(f"  Resolved version: {version}")

        raw = retry_request(
            lambda: _gfw_query_alerts(version, DATA_FROM_DATE, AREA_OF_INTEREST),
            source, "query alerts"
        )
        print(f"  Fetched {len(raw)} alert records.")

        df = _gfw_parse(raw)
        print(f"  Parsed {len(df)} rows.")

        load_dataframe_to_bigquery(df, GFW_TABLE_ID, GFW_SCHEMA)
        print(f"[{source}] Done.")

    except Exception as e:
        print(f"[{source}] FAILED: {e}")
        send_failure_alert(source, e)


# ===========================================================================
# Pipeline 3: NASA Earthdata -> Vegetation / Biomass (MODIS NDVI)
# ===========================================================================
# Requires: NASA Earthdata account (NASA_EARTHDATA_USERNAME / NASA_EARTHDATA_PASSWORD)
# Product:  MOD13A3 v061 - MODIS/Terra Vegetation Indices Monthly L3
# Table:    nasa_vegetation_biomass
# ---------------------------------------------------------------------------

NASA_TABLE_ID = "nasa_vegetation_biomass"
NASA_CMR_URL  = "https://cmr.earthdata.nasa.gov/search/granules.json"

NASA_SCHEMA = [
    bigquery.SchemaField("granule_id",          "STRING"),
    bigquery.SchemaField("product",             "STRING"),
    bigquery.SchemaField("start_date",          "STRING"),
    bigquery.SchemaField("end_date",            "STRING"),
    bigquery.SchemaField("bounding_box",        "STRING"),
    bigquery.SchemaField("download_url",        "STRING"),
    bigquery.SchemaField("file_size_mb",        "STRING"),
    bigquery.SchemaField("ingestion_timestamp", "TIMESTAMP"),
]


def _bbox_from_geometry(geometry: dict) -> tuple:
    """Derive (min_lon, min_lat, max_lon, max_lat) from a GeoJSON polygon."""
    coords = geometry["coordinates"][0]
    lons   = [c[0] for c in coords]
    lats   = [c[1] for c in coords]
    return (min(lons), min(lats), max(lons), max(lats))


def _nasa_search_granules(bbox: tuple, from_date: str) -> list[dict]:
    """
    Search CMR for MOD13A3 granules overlapping the area and date range.
    CMR search is a public endpoint — no authentication required.
    """
    params = {
        "short_name":   "MOD13A3",
        "version":      "061",
        "temporal":     f"{from_date}T00:00:00Z,",
        "bounding_box": ",".join(str(x) for x in bbox),
        "page_size":    100,
    }
    resp = requests.get(
        NASA_CMR_URL, params=params,
        headers={"User-Agent": "forest-monitoring-pipeline/1.0"},
        timeout=60,
    )
    resp.raise_for_status()
    return resp.json().get("feed", {}).get("entry", [])


def _nasa_parse(entries: list[dict]) -> pd.DataFrame:
    now  = datetime.now(timezone.utc)
    rows = []
    for e in entries:
        links     = e.get("links", [])
        data_link = next(
            (l["href"] for l in links if l.get("rel") == "http://esipfed.org/ns/fedsearch/1.1/data#"),
            ""
        )
        size_str  = e.get("granule_size", "0")
        rows.append({
            "granule_id":          e.get("id", ""),
            "product":             e.get("short_name", "MOD13A3"),
            "start_date":          e.get("time_start", "")[:10],
            "end_date":            e.get("time_end",   "")[:10],
            "bounding_box":        e.get("boxes", [""])[0],
            "download_url":        data_link,
            "file_size_mb":        size_str if size_str else None,
            "ingestion_timestamp": now,
        })
    return pd.DataFrame(rows)


def run_nasa_pipeline() -> None:
    source = "NASA Earthdata (MODIS NDVI)"
    print(f"\n[{source}] Starting...")
    try:
        bbox = _bbox_from_geometry(AREA_OF_INTEREST)

        entries = retry_request(
            lambda: _nasa_search_granules(bbox, DATA_FROM_DATE),
            source, "CMR granule search"
        )
        print(f"  Found {len(entries)} granules.")

        df = _nasa_parse(entries)
        print(f"  Parsed {len(df)} rows.")

        load_dataframe_to_bigquery(df, NASA_TABLE_ID, NASA_SCHEMA)
        print(f"[{source}] Done.")

    except Exception as e:
        print(f"[{source}] FAILED: {e}")
        send_failure_alert(source, e)


# ===========================================================================
# Main
# ===========================================================================

if __name__ == "__main__":
    print("=" * 60)
    print("Forest Monitoring Ingestion Pipeline")
    print(f"Run time: {datetime.now(timezone.utc).isoformat()}")
    print("=" * 60)

    # Each pipeline runs independently regardless of others succeeding or failing
    run_gee_pipeline()
    run_gfw_pipeline()
    run_nasa_pipeline()

    print("\n" + "=" * 60)
    print("All pipelines completed (check above for individual statuses).")
    print("=" * 60)
