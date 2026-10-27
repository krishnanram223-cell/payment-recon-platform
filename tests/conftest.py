import pytest


def _shutdown_jvm() -> None:
    """End the Java process behind PySpark, so the next test file starts a brand-new one.

    session.stop() ends the SparkContext but keeps the same JVM alive for the whole
    pytest run. Whatever that JVM never gives back (streaming state, Delta caches,
    classes from many sessions) piles up file after file until it dies. A fresh JVM
    per test file costs a few seconds and makes every file start clean.
    """
    from pyspark import SparkContext

    gateway = SparkContext._gateway
    if gateway is None:
        return
    proc = getattr(gateway, "proc", None)
    gateway.shutdown()
    if proc is not None:
        proc.stdin.close()  # the JVM exits when Python closes its stdin
        try:
            proc.wait(timeout=60)
        except Exception:
            proc.kill()
    SparkContext._gateway = None
    SparkContext._jvm = None


@pytest.fixture(scope="module")
def spark():
    """A fresh SparkSession AND a fresh JVM per test FILE."""
    from payrecon.common.spark import get_spark

    session = get_spark("payrecon-tests", {"spark.ui.enabled": "false"})
    yield session
    session.stop()
    _shutdown_jvm()
