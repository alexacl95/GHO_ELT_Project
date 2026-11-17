import sys

from dags.api.gho_client import (_atomic_write_json,
                        build_session,
                        get_countries,
                        get_indicator_catalog,
                        download_indicators,
                        save_raw_data)

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

    countries = get_countries(session)
    _atomic_write_json({"value": countries}, run_dir / "_countries.json")
    


