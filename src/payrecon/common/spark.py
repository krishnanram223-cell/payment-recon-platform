"""Local SparkSession with Delta Lake enabled."""
from __future__ import annotations

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def get_spark(app_name: str = "payrecon") -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")  # use all local cores; no cluster needed
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")  # default 200 is far too many for small local data
        .config("spark.sql.session.timeZone", "UTC")  # payments are always reasoned about in UTC
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")  # invalid dates -> NULL with try_ functions, never old lenient parsing
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.driver.memory", "2g")
    )
    # Adds the Delta Lake jar matching the installed delta-spark version (downloaded once from Maven).
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
