from pyspark import core

import pytest
from dags.DB.load import build_core, create_schema_and_tables, get_conn_cursor
pytestmark = pytest.mark.integration


@pytest.fixture
def cur():
    """
    Description: Fixture to provide a database cursor for testing, with setup and teardown of the database state.
    Output: Database cursor object
    """
    conn, cur = get_conn_cursor()
    create_schema_and_tables(cur)
    cur.execute("TRUNCATE staging.gho_values, staging.gho_countries, staging.gho_indicators")
    cur.execute("TRUNCATE core.gho_values, core.gho_countries, core.gho_indicators")
    yield cur
    conn.rollback()
    conn.close()


def test_core_is_idempotent_and_drops_null_values(cur):
    """
    Description: Test that the core table build process is idempotent and does not insert rows with NULL values.
    Input: Database cursor object
    """
    cur.execute("""
        INSERT INTO staging.gho_values
          (indicator_code, country_code, year, dim1, dim2, dim3, value) VALUES
          ('X','COL',2020,'SEX_MLE','','', 70.5),
          ('X','COL',2021,'','','', NULL)
    """)
    first = build_core(cur)
    second = build_core(cur)

    assert first["values_upserted"] == 1       # the NULL row is not in core
    assert second["values_upserted"] == 0      # nothing changed
    assert second["values_deleted"] == 0
    cur.execute("SELECT sex FROM core.gho_values")
    assert cur.fetchone()["sex"] == "Male"