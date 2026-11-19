import sys
from api.gho_client import (_atomic_write_json,
                            build_session,
                            get_countries,
                            get_indicator_catalog,
                            download_indicators,
                            save_raw_data, save_reference_data)

from DB.load import read_catalog, write_dimensions
from datatransformation.gho_transform import (build_spark,
                                            clean,
                                            read_countries,
                                            read_raw,
                                            write_parquet)
from validate.validate import validate

from pathlib import Path
import logging

import json, glob

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
    save_reference_data(run_dir, catalog, countries)

    #logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    out_dir = Path("data/processed/gho_indicators/")

    spark = build_spark()
    try:
        countries = read_countries(spark, run_dir)
        catalog = read_catalog(spark, run_dir)

        write_dimensions(countries, catalog, out_dir)
        cleaned = clean(read_raw(spark, run_dir), countries)
        write_parquet(cleaned, out_dir)   # facts: path unchanged
    finally:
        spark.stop() 
    
    #Validate the processed dataset
    validate(Path("data/processed/gho_indicators/gho_indicators"))