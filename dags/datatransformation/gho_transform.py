from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType, DoubleType, LongType, StringType, StructField, StructType,
)

log = logging.getLogger(__name__)

# Description: These are the fields we need. Spark ignores everything else in the JSON.
ROW_SCHEMA = StructType([
    StructField("Id", LongType()),
    StructField("IndicatorCode", StringType()),
    StructField("SpatialDimType", StringType()),
    StructField("SpatialDim", StringType()),
    StructField("TimeDimType", StringType()),
    StructField("TimeDim", LongType()),
    StructField("Dim1", StringType()),
    StructField("Dim2", StringType()),
    StructField("Dim3", StringType()),
    StructField("Value", StringType()),
    StructField("NumericValue", DoubleType()),
    StructField("Low", DoubleType()),
    StructField("High", DoubleType()),
])

# Description: The GHO API returns a JSON object with a "value" array of rows and metadata.
FILE_SCHEMA = StructType([
    StructField("indicator_code", StringType()),
    StructField("extracted_at", StringType()),
    StructField("value", ArrayType(ROW_SCHEMA)),
])

# Description: The country dimension is a JSON object with a "value" array of country rows.
COUNTRY_SCHEMA = StructType([
    StructField("value", ArrayType(StructType([
        StructField("country_code", StringType()),
        StructField("country_name", StringType()),
        StructField("who_region_code", StringType()),
        StructField("who_region_name", StringType()),
    ]))),
])

CATALOG_SCHEMA = StructType([
    StructField("IndicatorCode", StringType()),
    StructField("IndicatorName", StringType()),
])

def build_spark() -> SparkSession:
    """
    Description: Build a SparkSession for data transformation using local Spark.
    Output: SparkSession object
    """
    log.info("Building local Spark session")
    return (
        SparkSession.builder
        .master("local[*]")
        .appName("gho-transform")
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.driver.host", "127.0.0.1")  # Ensure local mode
        .config("spark.submit.deployMode", "client")  # Force client mode
        .getOrCreate()
    )


def read_raw(spark: SparkSession, run_dir: Path) -> DataFrame:
    """
    Description: Read every indicator file of a run and flatten to one row per observation. 
    [!_]*.json skips _manifest.json and _countries.json
    Input: 
        spark: SparkSession object
        run_dir: Path object
    Output: DataFrame object
    """
    raw = (
        spark.read.schema(FILE_SCHEMA)
        .option("multiLine", True)
        .json(str(run_dir / "[!_]*.json"))
    )
    return (
        raw.select(F.explode("value").alias("r"), "extracted_at")
        .select("r.*", "extracted_at")
    )


def read_countries(spark: SparkSession, run_dir: Path) -> DataFrame:
    """
    Description: Read the country dimension file of a run and flatten to one row per country.
    Input: 
        spark: SparkSession object
        run_dir: Path object
    Output: DataFrame object
    """
    raw = (
        spark.read.schema(COUNTRY_SCHEMA)
        .option("multiLine", True)
        .json(str(run_dir / "_countries.json"))
    )
    return raw.select(F.explode("value").alias("c")).select("c.*")


def clean(df: DataFrame, countries: DataFrame) -> DataFrame:
    """
    Description: Standardize names and types, keep country-level yearly rows, dedupe.
    Input: 
        df: DataFrame object
        countries: DataFrame object
    Output: DataFrame object
    """
    df = df.select(
        F.col("Id").alias("source_id"),
        F.upper(F.trim("IndicatorCode")).alias("indicator_code"),
        F.col("SpatialDimType").alias("spatial_type"),
        F.upper(F.trim("SpatialDim")).alias("country_code"),
        F.col("TimeDimType").alias("time_type"),
        F.col("TimeDim").cast("int").alias("year"),
        # null dims -> '' so the composite primary key behaves in Postgres
        F.coalesce(F.trim("Dim1"), F.lit("")).alias("dim1"),
        F.coalesce(F.trim("Dim2"), F.lit("")).alias("dim2"),
        F.coalesce(F.trim("Dim3"), F.lit("")).alias("dim3"),
        F.col("Value").alias("value_display"),
        F.col("NumericValue").alias("value"),
        F.col("Low").alias("value_low"),
        F.col("High").alias("value_high"),
        "extracted_at",
    )

    total = df.count()
    df = df.filter(
        (F.col("spatial_type") == "COUNTRY")
        & (F.col("time_type") == "YEAR")
        & F.col("year").isNotNull()
    )
    kept = df.count()
    log.info("rows read=%s, kept after country/year filter=%s, dropped=%s", total, kept, total - kept)

    # Same observation twice (e.g. overlapping files): keep one, deterministically.
    key = ["indicator_code", "country_code", "year", "dim1", "dim2", "dim3"]
    df = df.dropDuplicates(key)

    # Left join: we want to SEE rows whose country code is missing from the dimension.
    return df.join(F.broadcast(countries), on="country_code", how="left").drop(
        "spatial_type", "time_type"
    )


def write_parquet(df: DataFrame, out_dir: Path) -> None:
    """
    Description: Write the cleaned DataFrame to Parquet, partitioned by indicator_code.
    Input: 
        df: DataFrame object
        out_dir: Path object
    Output: None
    """
    (
        df.repartition("indicator_code")  # one file per indicator folder, not hundreds
        .write.mode("overwrite")
        .partitionBy("indicator_code")
        .parquet(str(out_dir))
    )
