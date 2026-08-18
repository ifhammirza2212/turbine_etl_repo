import os
from datetime import datetime

from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType

from turbine_etl.etl_metadata import load_metadata, save_metadata
from turbine_etl.logger_setup import get_logger, had_warnings
from turbine_etl.schema import (
    PRE_CONFORMED_SCHEMA,
    SUMMARY_STATS_SCHEMA,
    TURBINE_CONFORMED_SCHEMA,
)
from turbine_etl.transformers import (
    BRONZE_TABLE_PATH,
    GOLD_TABLE_PATH,
    SILVER_TABLE_PATH,
    SUMMARY_TABLE_PATH,
    Conform,
    Curate,
    Ingest,
    StatsSummary,
)

logger = get_logger()

DB_PATH = os.path.join("etl_outputs", "turbine_db")
RAW_DATA_DIR = "raw_data"
CSV_FILENAMES = ["data_group_1.csv", "data_group_2.csv", "data_group_3.csv"]

TABLES = {
    "turbine_raw": PRE_CONFORMED_SCHEMA,
    "turbine_curated": PRE_CONFORMED_SCHEMA,
    "turbine_conformed": TURBINE_CONFORMED_SCHEMA,
    "summary_stats": SUMMARY_STATS_SCHEMA,
}


def build_spark_session() -> SparkSession:
    builder = (
        SparkSession.builder.appName("turbine_etl")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def struct_type_from_schema(schema: dict) -> StructType:
    return StructType(
        [StructField(name, field["dtype"], nullable=field["nullable"]) for name, field in schema.items()]
    )


def create_table_if_missing(spark: SparkSession, table_name: str, schema: dict) -> None:
    table_path = os.path.join(DB_PATH, table_name)
    if DeltaTable.isDeltaTable(spark, table_path):
        return
    empty_df = spark.createDataFrame([], struct_type_from_schema(schema))
    empty_df.write.format("delta").save(table_path)


def instantiate_turbine_db(spark: SparkSession) -> None:
    os.makedirs(DB_PATH, exist_ok=True)
    for table_name, schema in TABLES.items():
        create_table_if_missing(spark, table_name, schema)


def count_delta_rows(spark: SparkSession, table_path: str) -> int:
    if not DeltaTable.isDeltaTable(spark, table_path):
        return 0
    return spark.read.format("delta").load(table_path).count()


def log_csv_row_counts(spark: SparkSession, csv_paths: list[str], previous_counts: dict) -> dict:
    current_counts = {}
    for path in csv_paths:
        filename = os.path.basename(path)
        row_count = spark.read.option("header", True).csv(path).count()
        previous_count = previous_counts.get(filename, 0)
        current_counts[filename] = row_count
        logger.info(
            "Read %d rows from %s (previous run: %d rows, increase: %d)",
            row_count,
            filename,
            previous_count,
            row_count - previous_count,
        )
    return current_counts


def run_layer_step(spark: SparkSession, step_name: str, table_path: str, step_fn) -> None:
    before_count = count_delta_rows(spark, table_path)
    step_fn()
    after_count = count_delta_rows(spark, table_path)
    logger.info(
        "%s completed successfully (%s rows before: %d, after: %d, increase: %d)",
        step_name,
        table_path,
        before_count,
        after_count,
        after_count - before_count,
    )


def main() -> None:
    spark = build_spark_session()
    errors_occurred = False

    logger.info("Pipeline execution started at %s", datetime.now().isoformat())

    # The entire pipeline runs inside this try/except so that every exception - whether a
    # deliberate schema/DQ error raised by a transformer (see error_classes.py) or a genuinely
    # unexpected bug - funnels through this single except block and gets logged here. Without
    # this, each transformer would need its own logging call at every raise site to guarantee
    # failures end up in the logfile. The exception is re-raised after logging so the run still
    # fails loudly (non-zero exit code) rather than being silently swallowed.
    try:
        instantiate_turbine_db(spark)

        csv_paths = [os.path.join(RAW_DATA_DIR, filename) for filename in CSV_FILENAMES]
        metadata = load_metadata()
        current_csv_counts = log_csv_row_counts(spark, csv_paths, metadata.get("csv_row_counts", {}))

        run_layer_step(spark, "Ingest", BRONZE_TABLE_PATH, lambda: Ingest(spark, csv_paths).run())
        run_layer_step(spark, "Curate", SILVER_TABLE_PATH, lambda: Curate(spark).run())
        run_layer_step(spark, "Conform", GOLD_TABLE_PATH, lambda: Conform(spark).run())
        run_layer_step(spark, "StatsSummary", SUMMARY_TABLE_PATH, lambda: StatsSummary(spark).run())

        save_metadata({"csv_row_counts": current_csv_counts})
    except Exception:
        errors_occurred = True
        logger.exception("turbine_etl pipeline run failed")
        raise
    finally:
        logger.info("Overall success: %s", not errors_occurred)
        logger.info("Errors occurred: %s", errors_occurred)
        logger.info("Warnings occurred: %s", had_warnings())
        spark.stop()


if __name__ == "__main__":
    main()
