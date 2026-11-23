from dags.datatransformation.gho_transform import clean
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType


def _facts(spark, rows):
    """
    Description: Create a Spark DataFrame with the schema used for GHO fact data.
    Input:
        1. spark: SparkSession object
        2. rows: List of tuples representing the rows of data
    Output: Spark DataFrame with the specified schema
    """
    schema = StructType([
        StructField("Id", IntegerType(), True),
        StructField("IndicatorCode", StringType(), True),
        StructField("SpatialDimType", StringType(), True),
        StructField("SpatialDim", StringType(), True),
        StructField("TimeDimType", StringType(), True),
        StructField("TimeDim", IntegerType(), True),
        StructField("Dim1", StringType(), True),
        StructField("Dim2", StringType(), True),
        StructField("Dim3", StringType(), True),
        StructField("Value", StringType(), True),
        StructField("NumericValue", DoubleType(), True),
        StructField("Low", DoubleType(), True),
        StructField("High", DoubleType(), True),
        StructField("extracted_at", StringType(), True),
    ])
    return spark.createDataFrame(rows, schema)


def _countries(spark):
    """
    Description: Create a Spark DataFrame with the schema used for GHO country dimension data.
    Input:
        1. spark: SparkSession object
    Output: Spark DataFrame with the specified schema
    """
    return spark.createDataFrame(
        [("COL", "Colombia", "AMR", "Americas")],
        ["country_code", "country_name", "who_region_code", "who_region_name"],
    )


def test_keeps_only_country_year_rows(spark):
    """
    Description: Test that the clean function retains only rows with SpatialDimType 'COUNTRY' and TimeDimType 'YEAR'.
    Input:
        1. spark: SparkSession object
    Output: Assertion that the cleaned DataFrame contains only the expected rows
    """
    df = _facts(spark, [
        (1, "X", "COUNTRY", "COL", "YEAR", 2020, None, None, None, "1", 1.0, None, None, "t"),
        (2, "X", "REGION",  "AMR", "YEAR", 2020, None, None, None, "1", 1.0, None, None, "t"),
    ])
    out = clean(df, _countries(spark)).collect()
    assert [r.country_code for r in out] == ["COL"]


def test_null_dims_become_empty_strings(spark):
    """
    Description: Test that the clean function converts NULL values in Dim1, Dim2, and Dim3 to empty strings.
    Input:
        1. spark: SparkSession object
    Output: Assertion that the cleaned DataFrame has empty strings for Dim1, Dim2, and Dim3, and that indicator_code and country_code are normalized to uppercase
    """
    df = _facts(spark, [
        (1, "x", "COUNTRY", "col", "YEAR", 2020, None, None, None, "1", 1.0, None, None, "t"),
    ])
    row = clean(df, _countries(spark)).collect()[0]
    assert (row.dim1, row.dim2, row.dim3) == ("", "", "")
    assert row.indicator_code == "X" and row.country_code == "COL"   # normalised


def test_duplicate_observations_are_collapsed(spark):
    """
    description: Test that the clean function collapses duplicate observations into a single row.
    Input:
        1. spark: SparkSession object
    Output: Assertion that the cleaned DataFrame contains only one row when duplicate observations are present
    """
    r = (1, "X", "COUNTRY", "COL", "YEAR", 2020, None, None, None, "1", 1.0, None, None, "t")
    assert clean(_facts(spark, [r, r]), _countries(spark)).count() == 1
