import pytest


@pytest.fixture(scope="module")
def spark():
    """A fresh SparkSession per test FILE.

    Stopping it after each file releases everything the session held in memory
    (cached data, checkpointed results, job history), so a long test run can't
    slowly exhaust the Java heap. Starting Spark again costs a few seconds per file.
    """
    from payrecon.common.spark import get_spark

    session = get_spark("payrecon-tests", {"spark.ui.enabled": "false"})
    yield session
    session.stop()
