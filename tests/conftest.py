import pytest


@pytest.fixture(scope="session")
def spark():
    """A small local Spark session shared by all Spark tests."""
    pyspark = pytest.importorskip("pyspark")
    session = (
        pyspark.sql.SparkSession.builder.master("local[2]")
        .appName("grid-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()
