"""Load GHO data into PostgreSQL: Parquet -> staging -> core.
"""
from __future__ import annotations

import json
import logging
import os
import sys
from pathlib import Path

from datatransformation.gho_transform import CATALOG_SCHEMA
import psycopg
import pyarrow.dataset as ds
from psycopg.rows import dict_row

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
                                ArrayType,
                                DoubleType,
                                LongType, 
                                StringType, 
                                StructField, 
                                StructType,
                                )

log = logging.getLogger(__name__)

SCHEMA_FILE = Path(__file__).resolve().parent.parent / "DB" / "schema.sql"
PROCESSED_DIR = "data/processed/gho_indicators"
BATCH_ROWS = 50_000

FACT_COLS = [
    "indicator_code", "country_code", "year", "dim1", "dim2", "dim3",
    "value", "value_low", "value_high", "value_display", "source_id", "extracted_at",
]


def get_conn_cursor():
    """
    Description: Establish a connection to the PostgreSQL database and return a cursor with dict_row factory.
    Output: Tuple containing the connection and cursor objects.
    Note: This function uses environment variables for database connection parameters.
    """
    conn = psycopg.connect(
        host=os.environ["POSTGRES_CONN_HOST"], port=os.environ["POSTGRES_CONN_PORT"],
        dbname=os.environ["ELT_DATABASE_NAME"], user=os.environ["ELT_DATABASE_USERNAME"],
        password=os.environ["ELT_DATABASE_PASSWORD"],
    )
    cur = conn.cursor(row_factory=dict_row)
    return conn, cur


def close_conn_cursor(conn, cur) -> None:
    """
    Description: Close the cursor and connection to the PostgreSQL database.
    Input:
        1. conn: Connection object to the PostgreSQL database.
        2. cur: Cursor object associated with the connection.
    """
    cur.close()
    conn.close()

def create_schema_and_tables(cur) -> None:
    """
    Description: Create both schemas and all tables from sql/schema.sql (idempotent).
    Input: cur: Cursor object associated with the connection.
    """
    cur.execute(SCHEMA_FILE.read_text(encoding="utf-8"))

def read_catalog(spark: SparkSession, run_dir: Path) -> DataFrame:
    """description: Read _catalog.json (a plain JSON list) into the indicator dimension.
    input: 
        1. spark: SparkSession object.
        2. run_dir: Path object representing the run directory.
    output: DataFrame containing the indicator catalog with normalized indicator codes and names.
    """
    return (
        spark.read.schema(CATALOG_SCHEMA)
        .option("multiLine", True)
        .json(str(run_dir / "_catalog.json"))
        .select(
            F.upper(F.trim("IndicatorCode")).alias("indicator_code"),  # same normalisation as the facts
            F.trim("IndicatorName").alias("indicator_name"),
        )
        .filter(F.col("indicator_code").isNotNull())
        .dropDuplicates(["indicator_code"])
    )

def read_countries(spark: SparkSession, run_dir: Path) -> DataFrame:
    """
    Description: Read _countries.json (a wrapped JSON list) into the country dimension.
    Input:
        1. spark: SparkSession object.
        2. run_dir: Path object representing the run directory.
    Output: DataFrame containing the country information.
    """
    raw = (
        spark.read.schema(COUNTRY_SCHEMA)
        .option("multiLine", True)
        .json(str(run_dir / "_countries.json"))
    )
    return (
        raw.select(F.explode("value").alias("c")).select("c.*")
        .withColumn("country_code", F.upper(F.trim("country_code")))
        .dropDuplicates(["country_code"])
    )

def write_dimensions(countries: DataFrame, catalog: DataFrame, out_root: Path) -> None:
    """Dimensions are tiny: one Parquet file each."""
    for df, name in [(countries, "dim_countries"), (catalog, "dim_indicators")]:
        df.coalesce(1).write.mode("overwrite").parquet(str(out_root / name))

# ======================================================================
# data_loading: read the small JSON side files of a run
# ======================================================================
def _read_json(path: Path):
    """
    Description: Read a JSON file and return its content as a Python object.
    Input:
        1. path: Path object representing the JSON file to be read.
    Output: Python object (dict or list) containing the parsed JSON data.
    """
    try:
        log.info("Processing file: %s", path)
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        log.error("file not found: %s", path)
        raise
    except json.JSONDecodeError:
        log.error("Invalid JSON in file: %s", path)
        raise


def load_data(run_dir: Path) -> tuple[list[dict], list[dict]]:
    """Description: Return (countries, catalog) saved in the raw run folder.
    input: run_dir: Path object representing the run directory.
    output: Tuple containing two lists of dictionaries: countries and catalog."""

    countries = _read_json(run_dir / "_countries.json")["value"]
    catalog = _read_json(run_dir / "_catalog.json")
    if isinstance(catalog, dict):          # accept {"catalog": [...]} or a plain list
        catalog = catalog["catalog"]
    return countries, catalog


# ======================================================================
# data_modification: staging writes (COPY, not row-by-row INSERT)
# ======================================================================
def truncate_staging(cur) -> None:
    """
    Description: Empty the staging tables before reloading. 
    TRUNCATE is transactional in Postgres: if the load fails, the old data stays.
    input: cur: Cursor object associated with the connection.
    """
    cur.execute(
        "TRUNCATE staging.gho_values, staging.gho_countries, staging.gho_indicators"
    )


def copy_dimensions(cur, countries: list[dict], catalog: list[dict]) -> None:
    """
    Description: Stream the country and indicator dimensions into staging tables using COPY.
    Input:
        1. cur: Cursor object associated with the connection.
        2. countries: List of dictionaries containing country information.
        3. catalog: List of dictionaries containing indicator information.
        Output: None. The function performs the COPY operation to load data into the staging tables."""
    with cur.copy(
        "COPY staging.gho_countries "
        "(country_code, country_name, who_region_code, who_region_name) FROM STDIN"
    ) as copy:
        for c in countries:
            copy.write_row((
                c["country_code"], c["country_name"],
                c["who_region_code"], c["who_region_name"],
            ))

    with cur.copy(
        "COPY staging.gho_indicators (indicator_code, indicator_name) FROM STDIN"
    ) as copy:
        for c in catalog:
            copy.write_row((c["IndicatorCode"], c["IndicatorName"]))


def copy_facts(cur, parquet_dir: str) -> int:
    """
    Description: Stream Parquet batches into staging.gho_values with COPY. 
    input:
        1. cur: Cursor object associated with the connection.
        2. parquet_dir: String representing the directory containing Parquet files. 
    output: row count.
    """
    dataset = ds.dataset(parquet_dir, format="parquet", partitioning="hive")
    total = 0
    copy_sql = f"COPY staging.gho_values ({', '.join(FACT_COLS)}) FROM STDIN"
    with cur.copy(copy_sql) as copy:
        for batch in dataset.to_batches(columns=FACT_COLS, batch_size=BATCH_ROWS):
            data = batch.to_pydict()
            for row in zip(*(data[c] for c in FACT_COLS)):
                copy.write_row(row)        # None becomes NULL
            total += batch.num_rows
            log.info("copied %s rows to staging so far", total)
    return total


# ======================================================================
# data_transformation: staging -> core, expressed as set-based SQL
# ======================================================================
UPSERT_CORE_INDICATORS = """
INSERT INTO core.gho_indicators AS t (indicator_code, indicator_name)
SELECT indicator_code, indicator_name FROM staging.gho_indicators
ON CONFLICT (indicator_code) DO UPDATE SET indicator_name = EXCLUDED.indicator_name
WHERE t.indicator_name IS DISTINCT FROM EXCLUDED.indicator_name
"""

UPSERT_CORE_COUNTRIES = """
INSERT INTO core.gho_countries AS t
    (country_code, country_name, who_region_code, who_region_name)
SELECT country_code, country_name, who_region_code, who_region_name
FROM staging.gho_countries
ON CONFLICT (country_code) DO UPDATE SET
    country_name = EXCLUDED.country_name,
    who_region_code = EXCLUDED.who_region_code,
    who_region_name = EXCLUDED.who_region_name
WHERE (t.country_name, t.who_region_code, t.who_region_name)
      IS DISTINCT FROM
      (EXCLUDED.country_name, EXCLUDED.who_region_code, EXCLUDED.who_region_name)
"""

# The transformation: drop null measurements and derive `sex` from dim1
# (the analogue of Video_type in the YouTube project).
UPSERT_CORE_VALUES = """
INSERT INTO core.gho_values AS t
    (indicator_code, country_code, year, dim1, dim2, dim3, sex,
     value, value_low, value_high, value_display, source_id, extracted_at)
SELECT indicator_code, country_code, year, dim1, dim2, dim3,
       CASE dim1
            WHEN 'SEX_BTSX' THEN 'Both sexes'
            WHEN 'SEX_MLE'  THEN 'Male'
            WHEN 'SEX_FMLE' THEN 'Female'
       END,
       value, value_low, value_high, value_display, source_id, extracted_at
FROM staging.gho_values
WHERE value IS NOT NULL
ON CONFLICT (indicator_code, country_code, year, dim1, dim2, dim3) DO UPDATE SET
    value = EXCLUDED.value,
    value_low = EXCLUDED.value_low,
    value_high = EXCLUDED.value_high,
    value_display = EXCLUDED.value_display,
    source_id = EXCLUDED.source_id,
    extracted_at = EXCLUDED.extracted_at
WHERE (t.value, t.value_low, t.value_high, t.value_display)
      IS DISTINCT FROM
      (EXCLUDED.value, EXCLUDED.value_low, EXCLUDED.value_high, EXCLUDED.value_display)
"""

# "DELETE rows not in staging" (step 7 of your core_table), as anti-joins.
DELETE_STALE_VALUES = """
DELETE FROM core.gho_values t
WHERE NOT EXISTS (
    SELECT 1 FROM staging.gho_values s
    WHERE s.indicator_code = t.indicator_code AND s.country_code = t.country_code
      AND s.year = t.year AND s.dim1 = t.dim1 AND s.dim2 = t.dim2 AND s.dim3 = t.dim3
      AND s.value IS NOT NULL
)
"""
DELETE_STALE_COUNTRIES = """
DELETE FROM core.gho_countries t
WHERE NOT EXISTS (SELECT 1 FROM staging.gho_countries s WHERE s.country_code = t.country_code)
"""
DELETE_STALE_INDICATORS = """
DELETE FROM core.gho_indicators t
WHERE NOT EXISTS (SELECT 1 FROM staging.gho_indicators s WHERE s.indicator_code = t.indicator_code)
"""


def build_core(cur) -> dict[str, int]:
    """
    Description: Upsert core from staging, then delete rows that disappeared. Returns row counts."""
    counts: dict[str, int] = {}
    for name, sql in [
        ("indicators_upserted", UPSERT_CORE_INDICATORS),
        ("countries_upserted", UPSERT_CORE_COUNTRIES),
        ("values_upserted", UPSERT_CORE_VALUES),      # inserted OR changed rows only
        ("values_deleted", DELETE_STALE_VALUES),
        ("countries_deleted", DELETE_STALE_COUNTRIES),
        ("indicators_deleted", DELETE_STALE_INDICATORS),
    ]:
        cur.execute(sql)
        counts[name] = cur.rowcount
    return counts


# ======================================================================
# dwh: the two "tasks" (plain functions now, @task in Airflow later)
# ======================================================================
def staging_table(run_dir: Path) -> int:
    """Description: Raw run + Parquet -> staging. One transaction: all or nothing.

    Steps: 1 connect, 2 read side files, 3 create schemas/tables,
           4 truncate staging, 5 COPY dimensions and facts, 6 commit.
    """
    conn, cur = None, None
    try:
        conn, cur = get_conn_cursor()
        countries, catalog = load_data(run_dir)
        create_schema_and_tables(cur)
        truncate_staging(cur)
        copy_dimensions(cur, countries, catalog)
        n = copy_facts(cur, PROCESSED_DIR)
        conn.commit()
        log.info("staging table update completed: %s fact rows", n)
        return n
    except Exception as e:
        if conn:
            conn.rollback()
        log.error("An error occurred during the update of staging: %s", e)
        raise
    finally:
        if conn and cur:
            close_conn_cursor(conn, cur)


def core_table() -> dict[str, int]:
    """Description: staging -> core: transform, upsert, delete stale. One transaction."""
    conn, cur = None, None
    try:
        conn, cur = get_conn_cursor()
        create_schema_and_tables(cur)
        counts = build_core(cur)
        conn.commit()
        log.info("core table update completed: %s", counts)
        return counts
    except Exception as e:
        if conn:
            conn.rollback()
        log.error("An error occurred during the update of core: %s", e)
        raise
    finally:
        if conn and cur:
            close_conn_cursor(conn, cur)
