import sys

from gho_client import build_session, get_indicator_catalog, download_indicators, save_raw_data
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
RAW_DIR = Path("data/raw")


if __name__ == "__main__":
    session = build_session()

    # Catalog: look things up, and save a copy for reference

    catalog = get_indicator_catalog(session, "life expectancy")
    for item in catalog[:5]:
        print(item["IndicatorCode"], "|", item["IndicatorName"])

    # Download
    results, failures = download_indicators(session, [i["IndicatorCode"] for i in catalog])

    # Save what we got, even if some failed
    run_dir = save_raw_data(results, RAW_DIR, failures)
    print(f"Saved to {run_dir}")

    


