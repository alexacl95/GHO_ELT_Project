import sys
import pytest
from pyspark.sql import SparkSession

# Add project root to Python path so dags module can be imported
sys.path.insert(0, "/opt/airflow")


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
