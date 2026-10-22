import pytest


@pytest.fixture(scope="session")
def spark():
    """One local SparkSession shared by all tests (starting Spark takes seconds)."""
    from payrecon.common.spark import get_spark

    session = get_spark("payrecon-tests")
    yield session
    session.stop()
