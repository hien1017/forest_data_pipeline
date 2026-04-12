"""
Fact Table Refresh
==================
Refreshes two BigQuery fact tables by running CREATE OR REPLACE TABLE queries.
"""

import os
import sys
from datetime import datetime, timezone

from google.cloud import bigquery

PROJECT_ID = os.environ["PROJECT_ID"]

FACT_TABLE_JOBS = [
    (
        "nasa_vegetation_biomass_fct",
        f"""
        CREATE OR REPLACE TABLE `{PROJECT_ID}.fct_reporting.nasa_vegetation_biomass_fct` AS
        SELECT *
        FROM `{PROJECT_ID}.stg_cleaned.nasa_vegetation_biomass_cleaned_view`
        """,
    ),
    (
        "spatial_forest_analysis_fct",
        f"""
        CREATE OR REPLACE TABLE `{PROJECT_ID}.fct_reporting.spatial_forest_analysis_fct` AS
        SELECT
            gee.longitude,
            gee.latitude,
            COALESCE(gee.year, gfw.year) AS year,
            gee.loss_area_ha,
            gee.gain_area_ha,
            gee.tree_cover_pct,
            gfw.alert_date,
            gfw.intensity,
            gfw.confidence
        FROM `{PROJECT_ID}.stg_cleaned.gee_forest_change_cleaned_view` gee
        LEFT JOIN `{PROJECT_ID}.stg_cleaned.gfw_deforestation_alerts_cleaned_view` gfw
            ON  gee.longitude = gfw.longitude
            AND gee.latitude  = gfw.latitude
            AND gee.year      = gfw.year
        """,
    ),
]


def main() -> None:
    print(f"Fact Table Refresh — {datetime.now(timezone.utc).isoformat()} UTC")

    client = bigquery.Client(project=PROJECT_ID)
    failed = []

    for name, query in FACT_TABLE_JOBS:
        print(f"\n[{name}] Running...")
        try:
            client.query(query).result()
            print(f"[{name}] Done.")
        except Exception as exc:
            print(f"[{name}] FAILED: {exc}", file=sys.stderr)
            failed.append(name)

    if failed:
        print(f"\nFailed tables: {failed}")
        sys.exit(1)

    print("\nAll fact tables refreshed successfully.")


if __name__ == "__main__":
    main()
