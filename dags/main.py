import sys

from api.gho_client import (_atomic_write_json,
                        build_session,
                        get_countries,
                        get_indicator_catalog,
                        download_indicators,
                        save_raw_data)

from pathlib import Path
import logging

import json, glob

from datatransformation.gho_transform import (build_spark,
                                                   clean,
                                                   read_countries,
                                                   read_raw,
                                                   write_parquet)


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

    f = glob.glob("data/raw/gho_*/WHOSIS_000001_*.json")[0]
    print(json.load(open(f))["value"][0])


    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run_dir = Path('data/raw/gho_2026-11-13')          # e.g. data/raw/gho_2026-11-13
    out_dir = Path("data/processed/gho_indicators")

    spark = build_spark()
    try:
        cleaned = clean(read_raw(spark, run_dir), read_countries(spark, run_dir))
        write_parquet(cleaned, out_dir)
    finally:
        spark.stop()
    


