"""
Shared pytest fixtures.

A single session-scoped SparkSession is reused across all tests instead of
creating a new one per test - spinning up the JVM is the slow part of any
PySpark test suite, so sharing it keeps the suite fast.
"""
import pytest

from pipelines.spark_session import get_spark_session


@pytest.fixture(scope="session")
def spark():
    session = get_spark_session(app_name="pyspark-databricks-lakehouse-tests")
    yield session
    session.stop()
