import os
from collections import Counter
from datetime import date, datetime, time, timedelta
from functools import reduce

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from turbine_etl.error_classes import (
    AnomalousEntryError,
    DataRangeSchemaError,
    DataTypeSchemaError,
    DuplicateEntryError,
    FieldNameSchemaError,
    IncorrectTimestampEntryError,
    MissingEntryError,
    UnexpectedEntryError,
)
from turbine_etl.logger_setup import get_logger
from turbine_etl.schema import PRE_CONFORMED_SCHEMA

logger = get_logger()

BRONZE_TABLE_PATH = os.path.join("etl_outputs", "turbine_db", "turbine_raw")
SILVER_TABLE_PATH = os.path.join("etl_outputs", "turbine_db", "turbine_curated")
GOLD_TABLE_PATH = os.path.join("etl_outputs", "turbine_db", "turbine_conformed")
SUMMARY_TABLE_PATH = os.path.join("etl_outputs", "turbine_db", "summary_stats")
SUMMARY_WINDOW_DAYS = 2
MEASUREMENT_ID_TIMESTAMP_FORMAT = "ddMMyyyy_HHmm"
ANOMALY_STDDEV_MULTIPLIER = 2
TIMESTAMP_FORMAT = "dd/MM/yyyy HH:mm"
TURBINE_ID_RANGE = range(1, 16)
HOUR_RANGE = range(24)


def _raise_if_out_of_range(df: DataFrame, schema: dict) -> None:
    for field_name, spec in schema.items():
        value_range = spec.get("range")
        if not value_range:
            continue

        min_value, max_value = value_range
        out_of_range_conditions = []
        if min_value is not None:
            out_of_range_conditions.append(F.col(field_name) < F.lit(min_value))
        if max_value is not None:
            out_of_range_conditions.append(F.col(field_name) > F.lit(max_value))
        if not out_of_range_conditions:
            continue

        is_out_of_range = reduce(lambda a, b: a | b, out_of_range_conditions)
        has_out_of_range_values = df.filter(F.col(field_name).isNotNull() & is_out_of_range).limit(1).count() > 0
        if has_out_of_range_values:
            raise DataRangeSchemaError(f"Column '{field_name}' contains values outside expected range {value_range}")


class Ingest:
    """Bronze-layer transformer: raw CSVs -> turbine_raw Delta table."""

    def __init__(self, spark: SparkSession, csv_paths: list[str]):
        self.spark = spark
        self.csv_paths = csv_paths
        self.schema = PRE_CONFORMED_SCHEMA

    def run(self) -> None:
        typed_dfs = []
        for path in self.csv_paths:
            raw_df = self._read_csv(path)
            self._validate_field_names(raw_df)
            typed_df = self._cast_and_validate_datatypes(raw_df)
            _raise_if_out_of_range(typed_df, self.schema)
            typed_dfs.append(typed_df)

        combined_df = reduce(DataFrame.unionByName, typed_dfs)
        combined_df.write.format("delta").mode("overwrite").save(BRONZE_TABLE_PATH)

    def _read_csv(self, path: str) -> DataFrame:
        return self.spark.read.option("header", True).csv(path)

    def _validate_field_names(self, df: DataFrame) -> None:
        expected_fields = set(self.schema.keys())
        actual_fields = set(df.columns)
        if actual_fields != expected_fields:
            raise FieldNameSchemaError(f"Expected fields {sorted(expected_fields)}, got {sorted(actual_fields)}")

    def _cast_and_validate_datatypes(self, df: DataFrame) -> DataFrame:
        typed_df = df
        for field_name, spec in self.schema.items():
            if field_name == "timestamp":
                casted_col = F.to_timestamp(F.col(field_name), TIMESTAMP_FORMAT)
            else:
                casted_col = F.col(field_name).cast(spec["dtype"])

            has_invalid_values = (
                df.filter(F.col(field_name).isNotNull() & (F.trim(F.col(field_name)) != "") & casted_col.isNull())
                .limit(1)
                .count()
                > 0
            )
            if has_invalid_values:
                raise DataTypeSchemaError(f"Column '{field_name}' contains values that cannot be cast to {spec['dtype']}")

            typed_df = typed_df.withColumn(field_name, casted_col)

        return typed_df


class Curate:
    """Silver-layer transformer: turbine_raw -> turbine_curated Delta table."""

    def __init__(self, spark: SparkSession, pipeline_run_date: date | None = None):
        self.spark = spark
        self.pipeline_run_date = pipeline_run_date or date.today()
        self.target_date = self.pipeline_run_date - timedelta(days=1)
        self.schema = PRE_CONFORMED_SCHEMA

    def run(self) -> None:
        full_df = self.spark.read.format("delta").load(BRONZE_TABLE_PATH)
        is_target_date = F.to_date(F.col("timestamp")) == F.lit(self.target_date)
        new_entries_df = full_df.filter(is_target_date)
        historical_df = full_df.filter(~is_target_date)

        new_entries_df = self._remove_incorrect_timestamps(new_entries_df)
        new_entries_df = self._input_missing_entries(new_entries_df)
        new_entries_df = self._remove_unexpected_entries(new_entries_df)
        _raise_if_out_of_range(new_entries_df, self.schema)

        output_df = historical_df.unionByName(new_entries_df)
        output_df.write.format("delta").mode("overwrite").save(SILVER_TABLE_PATH)

    def _expected_keys(self) -> set[tuple[int, datetime]]:
        return {
            (turbine_id, datetime.combine(self.target_date, time(hour=hour)))
            for turbine_id in TURBINE_ID_RANGE
            for hour in HOUR_RANGE
        }

    def _remove_incorrect_timestamps(self, df: DataFrame) -> DataFrame:
        is_target_date = F.to_date(F.col("timestamp")) == F.lit(self.target_date)
        for row in df.filter(~is_target_date).collect():
            logger.warning(str(IncorrectTimestampEntryError(timestamp=row.timestamp, today=self.target_date)))
        return df.filter(is_target_date)

    def _input_missing_entries(self, df: DataFrame) -> DataFrame:
        actual_keys = {(row.turbine_id, row.timestamp) for row in df.select("turbine_id", "timestamp").collect()}
        if not actual_keys:
            return df

        missing_keys = self._expected_keys() - actual_keys
        if not missing_keys:
            return df

        missing_rows = []
        for turbine_id, timestamp in missing_keys:
            logger.warning(str(MissingEntryError(turbine_id=turbine_id, timestamp=timestamp)))
            missing_rows.append((timestamp, turbine_id, None, None, None))

        missing_df = self.spark.createDataFrame(missing_rows, schema=df.schema)
        return df.unionByName(missing_df)

    def _remove_unexpected_entries(self, df: DataFrame) -> DataFrame:
        expected_keys = self._expected_keys()
        key_counts = Counter((row.turbine_id, row.timestamp) for row in df.select("turbine_id", "timestamp").collect())

        invalid_keys = {key for key in key_counts if key not in expected_keys}
        duplicate_valid_keys = {key for key, count in key_counts.items() if key in expected_keys and count > 1}

        for turbine_id, timestamp in invalid_keys | duplicate_valid_keys:
            logger.warning(str(UnexpectedEntryError(turbine_id=turbine_id, timestamp=timestamp, today=self.target_date)))

        if invalid_keys:
            invalid_keys_df = self.spark.createDataFrame(list(invalid_keys), schema=["turbine_id", "timestamp"])
            df = df.join(invalid_keys_df, on=["turbine_id", "timestamp"], how="left_anti")

        return df.dropDuplicates(["turbine_id", "timestamp"])


class Conform:
    """Gold-layer transformer: turbine_curated -> turbine_conformed Delta table."""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def run(self) -> None:
        silver_df = self.spark.read.format("delta").load(SILVER_TABLE_PATH)
        conformed_df = self._add_conformed_fields(silver_df)

        conformed_df = self._remove_duplicate_measurement_ids(conformed_df)
        self._log_anomalous_entries(conformed_df)

        conformed_df.write.format("delta").mode("overwrite").save(GOLD_TABLE_PATH)

    def _add_conformed_fields(self, df: DataFrame) -> DataFrame:
        df = df.withColumnRenamed("timestamp", "measurement_timestamp")
        measurement_id_col = F.concat(
            F.lit("t"),
            F.col("turbine_id").cast("string"),
            F.lit("_"),
            F.date_format(F.col("measurement_timestamp"), MEASUREMENT_ID_TIMESTAMP_FORMAT),
        )
        return df.withColumn("measurement_id", measurement_id_col).withColumn("pipeline_run_timestamp", F.current_timestamp())

    def _remove_duplicate_measurement_ids(self, df: DataFrame) -> DataFrame:
        duplicate_keys = (
            df.groupBy("measurement_id")
            .count()
            .filter(F.col("count") > 1)
            .join(df, on="measurement_id")
            .select("turbine_id", "measurement_timestamp")
            .distinct()
            .collect()
        )
        for row in duplicate_keys:
            logger.warning(str(DuplicateEntryError(turbine_id=row.turbine_id, timestamp=row.measurement_timestamp)))

        return df.dropDuplicates(["measurement_id"])

    def _log_anomalous_entries(self, df: DataFrame) -> None:
        """Log anomalous power_output readings; rows are kept, not removed, per AnomalousEntryError's
        raise_only handling - a statistical outlier may just mean a genuinely dysfunctional turbine
        that needs investigating, not bad data."""
        stats_df = df.groupBy("turbine_id").agg(
            F.avg("power_output").alias("mean_power_output"),
            F.stddev("power_output").alias("stddev_power_output"),
        )
        deviation = ANOMALY_STDDEV_MULTIPLIER * F.col("stddev_power_output")
        anomalous_rows = (
            df.join(stats_df, on="turbine_id")
            .filter(
                F.col("power_output").isNotNull()
                & (
                    (F.col("power_output") < F.col("mean_power_output") - deviation)
                    | (F.col("power_output") > F.col("mean_power_output") + deviation)
                )
            )
            .select("turbine_id", "measurement_timestamp")
            .collect()
        )
        for row in anomalous_rows:
            logger.warning(
                str(AnomalousEntryError(count=1, turbine_id=row.turbine_id, timestamp=row.measurement_timestamp))
            )


class StatsSummary:
    """Summary-layer transformer: turbine_conformed -> summary_stats Delta table."""

    def __init__(self, spark: SparkSession, pipeline_run_date: date | None = None):
        self.spark = spark
        self.pipeline_run_date = pipeline_run_date or date.today()

    def run(self) -> None:
        gold_df = self.spark.read.format("delta").load(GOLD_TABLE_PATH)
        window_start_date = self.pipeline_run_date - timedelta(days=SUMMARY_WINDOW_DAYS)
        windowed_df = gold_df.filter(F.to_date(F.col("measurement_timestamp")) >= F.lit(window_start_date))

        summary_df = (
            windowed_df.groupBy("turbine_id")
            .agg(
                F.min("power_output").alias("min_power_output"),
                F.max("power_output").alias("max_power_output"),
                F.avg("power_output").cast("float").alias("avg_power_output"),
            )
            .withColumn("calculation_date", F.lit(self.pipeline_run_date).cast("timestamp"))
        )

        summary_df.write.format("delta").mode("append").save(SUMMARY_TABLE_PATH)
