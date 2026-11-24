import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

# Add project root to Python path so dags module can be imported
# Works from any directory (tests/, ., /opt/airflow, etc.)
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


@pytest.fixture(scope="session")
def spark():
    """
    Description: Create a SparkSession for testing, with local master and minimal shuffle partitions.
    """
    session = (
        SparkSession.builder.master("local[1]")
        .appName("gho-tests")
        .config("spark.sql.shuffle.partitions", "2")   # default 200 makes tiny tests slow
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()
