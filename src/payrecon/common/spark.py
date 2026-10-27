"""Local SparkSession with Delta Lake and the Kafka connector."""
from __future__ import annotations

import pyspark
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

# Spark's Kafka connector. Its version must match the installed Spark exactly;
# Spark 4 is built for Scala 2.13, hence the "_2.13" in the name.
KAFKA_PACKAGE = f"org.apache.spark:spark-sql-kafka-0-10_2.13:{pyspark.__version__}"


def get_spark(app_name: str = "payrecon", extra_conf: dict[str, str] | None = None) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")  # use all local cores; no cluster needed
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")  # default 200 is far too many for small local data
        .config("spark.sql.session.timeZone", "UTC")  # payments are always reasoned about in UTC
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")  # invalid dates -> NULL with try_ functions
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.driver.memory", "2g")
        # Spark remembers past jobs, stages, tasks and queries for its web UI. A long-running
        # job (a stream, or a test suite) keeps adding to that history until memory runs out,
        # so keep only the recent part.
        .config("spark.ui.retainedJobs", "100")
        .config("spark.ui.retainedStages", "100")
        .config("spark.ui.retainedTasks", "5000")
        .config("spark.sql.ui.retainedExecutions", "50")
        .config("spark.streaming.ui.retainedBatches", "100")
    )
    for key, value in (extra_conf or {}).items():
        builder = builder.config(key, value)
    # Adds the Delta Lake jar matching the installed delta-spark version, plus the Kafka
    # connector (downloaded once from Maven, then cached). Packages can only be added when
    # the JVM starts, so every session gets both, even batch jobs that don't use Kafka.
    spark = configure_spark_with_delta_pip(builder, extra_packages=[KAFKA_PACKAGE]).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
