import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds

EXPECTED = {
    "indicator_code": pa.string(),   # comes from the hive partition folder name
    "source_id": pa.int64(),
    "country_code": pa.string(),
    "year": pa.int32(),
    "dim1": pa.string(), "dim2": pa.string(), "dim3": pa.string(),
    "value_display": pa.string(),
    "value": pa.float64(), "value_low": pa.float64(), "value_high": pa.float64(),
    "extracted_at": pa.string(),     # ISO string; Postgres casts it on COPY
    "country_name": pa.string(),
    "who_region_code": pa.string(),
    "who_region_name": pa.string(),
}


def validate(path: str = "data/processed/gho_indicators") -> None:
    """Check schema, row counts and unmatched country codes of the processed facts."""
    dataset = ds.dataset(path, format="parquet", partitioning="hive")    
    schema = dataset.schema
    problems = []
    for name, expected in EXPECTED.items():
        if name not in schema.names:
            problems.append(f"missing column: {name}")
        elif schema.field(name).type != expected:
            problems.append(f"{name}: expected {expected}, got {schema.field(name).type}")
    extra = set(schema.names) - set(EXPECTED)
    if extra:
        print(f"note: unexpected extra columns: {sorted(extra)}")
    if problems:
        raise AssertionError("Schema problems:\n  " + "\n  ".join(problems))

    table = dataset.to_table(
        columns=["indicator_code", "country_code", "year", "value", "country_name"]
    )
    n = table.num_rows
    n_indicators = len(pc.unique(table["indicator_code"]))
    unmatched = table["country_name"].null_count      # simpler than sum(is_null(...))
    null_values = table["value"].null_count

    print(f"rows={n:,}  indicators={n_indicators:,}")
    print(f"rows with null value={null_values:,} ({null_values / n:.1%})")
    print(f"rows with country code missing from dimension={unmatched:,}")