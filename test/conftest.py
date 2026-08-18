from datetime import date, datetime, time, timedelta

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from turbine_etl import transformers

TARGET_DATE = date(2022, 3, 1)


@pytest.fixture(scope="session")
def spark():
    builder = (
        SparkSession.builder.appName("turbine_etl-tests")
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    yield session
    session.stop()


@pytest.fixture(autouse=True)
def dummy_db_paths(tmp_path, monkeypatch):
    """Point each layer table at an isolated temp directory, so tests never touch etl_outputs/turbine_db."""
    paths = {
        "bronze": str(tmp_path / "turbine_raw"),
        "silver": str(tmp_path / "turbine_curated"),
        "gold": str(tmp_path / "turbine_conformed"),
        "summary": str(tmp_path / "summary_stats"),
    }
    monkeypatch.setattr(transformers, "BRONZE_TABLE_PATH", paths["bronze"])
    monkeypatch.setattr(transformers, "SILVER_TABLE_PATH", paths["silver"])
    monkeypatch.setattr(transformers, "GOLD_TABLE_PATH", paths["gold"])
    monkeypatch.setattr(transformers, "SUMMARY_TABLE_PATH", paths["summary"])
    return paths


@pytest.fixture
def pipeline_run_date():
    return TARGET_DATE + timedelta(days=1)


@pytest.fixture
def dummy_day_rows():
    """One clean day of data: 15 turbines x 24 hours = 360 records."""
    return [
        {
            "timestamp": datetime.combine(TARGET_DATE, time(hour=hour)),
            "turbine_id": turbine_id,
            "wind_speed": 10.0,
            "wind_direction": 180,
            "power_output": 2.5,
        }
        for turbine_id in range(1, 16)
        for hour in range(24)
    ]


@pytest.fixture
def write_csv(tmp_path):
    """Write rows (list of dicts) to a CSV file using the raw source header, return its path."""

    def _write(filename: str, rows: list[dict]) -> str:
        path = tmp_path / filename
        header = "timestamp,turbine_id,wind_speed,wind_direction,power_output"
        lines = [header] + [
            f"{row['timestamp']:%d/%m/%Y %H:%M},{row['turbine_id']},{row['wind_speed']},"
            f"{row['wind_direction']},{row['power_output']}"
            for row in rows
        ]
        path.write_text("\n".join(lines))
        return str(path)

    return _write


@pytest.fixture
def seed_table(spark):
    """Write rows (list of dicts) straight to a Delta table, with column order taken from the first row."""

    def _seed(path: str, rows: list[dict]) -> None:
        columns = list(rows[0].keys())
        tuples = [tuple(row[column] for column in columns) for row in rows]
        df = spark.createDataFrame(tuples, schema=columns)
        df.write.format("delta").mode("overwrite").save(path)

    return _seed
