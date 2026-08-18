import pytest

from turbine_etl.error_classes import DataRangeSchemaError, DataTypeSchemaError, FieldNameSchemaError
from turbine_etl.transformers import Ingest

HEADER = "timestamp,turbine_id,wind_speed,wind_direction,power_output"


def test_run_loads_valid_day_into_bronze(spark, dummy_db_paths, write_csv, dummy_day_rows):
    path = write_csv("data.csv", dummy_day_rows)

    Ingest(spark, [path]).run()

    bronze_df = spark.read.format("delta").load(dummy_db_paths["bronze"])
    assert bronze_df.count() == 360


def test_run_raises_on_wrong_field_name(spark, dummy_db_paths, tmp_path):
    path = tmp_path / "bad.csv"
    path.write_text("timestamp,turbine_id,wind_speed,wind_direction\n01/03/2022 00:00,1,10.0,180")

    with pytest.raises(FieldNameSchemaError):
        Ingest(spark, [str(path)]).run()


def test_run_raises_on_bad_datatype(spark, dummy_db_paths, tmp_path):
    path = tmp_path / "bad.csv"
    path.write_text(f"{HEADER}\n01/03/2022 00:00,1,not-a-number,180,2.5")

    with pytest.raises(DataTypeSchemaError):
        Ingest(spark, [str(path)]).run()


def test_run_raises_on_out_of_range_value(spark, dummy_db_paths, tmp_path):
    path = tmp_path / "bad.csv"
    path.write_text(f"{HEADER}\n01/03/2022 00:00,1,999,180,2.5")

    with pytest.raises(DataRangeSchemaError):
        Ingest(spark, [str(path)]).run()
