import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.compute as pc

EXPECTED = {
    "source_id": pa.int64(), "country_code": pa.string(), "year": pa.int32(),
    "dim1": pa.string(), "dim2": pa.string(), "dim3": pa.string(),
    "value_display": pa.string(), "value": pa.float64(),
    "value_low": pa.float64(), "value_high": pa.float64(),
    "country_name": pa.string(), "who_region_code": pa.string(),
    "who_region_name": pa.string(),
}

def validate(path: str = "data/processed/gho_indicators") -> None:
    """
    Description: Validate the processed GHO indicators dataset by checking schema, row counts, and unmatched country codes.
    Input:
      path (str): The path to the processed GHO indicators dataset.
    Output: None
    """
    dataset = ds.dataset(path, format="parquet", partitioning="hive")
    schema = dataset.schema

    for name, expected in EXPECTED.items():
        assert name in schema.names, f"missing column: {name}"
        actual = schema.field(name).type
        assert actual == expected, f"{name}: expected {expected}, got {actual}"

    table = dataset.to_table(columns=["indicator_code", "country_code", "year", "value", "country_name"])
    n = table.num_rows
    n_indicators = len(pc.unique(table["indicator_code"]))
    unmatched = pc.sum(pc.is_null(table["country_name"]).cast("int64")).as_py()
    null_values = table["value"].null_count

    print(f"rows={n:,}  indicators={n_indicators:,}")
    print(f"rows with null value={null_values:,} ({null_values / n:.1%})")
    print(f"rows with country code missing from dimension={unmatched:,}")

    