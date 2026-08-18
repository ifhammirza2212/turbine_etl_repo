from turbine_etl.transformers import Curate


def test_run_promotes_valid_day_to_silver(spark, dummy_db_paths, seed_table, dummy_day_rows, pipeline_run_date):
    seed_table(dummy_db_paths["bronze"], dummy_day_rows)

    Curate(spark, pipeline_run_date=pipeline_run_date).run()

    silver_df = spark.read.format("delta").load(dummy_db_paths["silver"])
    assert silver_df.count() == 360


def test_run_inputs_a_null_record_for_a_missing_entry(
    spark, dummy_db_paths, seed_table, dummy_day_rows, pipeline_run_date
):
    dropped_row = dummy_day_rows[-1]
    seed_table(dummy_db_paths["bronze"], dummy_day_rows[:-1])  # drop one expected entry

    Curate(spark, pipeline_run_date=pipeline_run_date).run()  # should not raise - logged and inputted instead

    silver_df = spark.read.format("delta").load(dummy_db_paths["silver"])
    assert silver_df.count() == 360  # still 360 - the gap was filled in, not left missing

    inputted_row = silver_df.filter(
        (silver_df.turbine_id == dropped_row["turbine_id"]) & (silver_df.timestamp == dropped_row["timestamp"])
    ).collect()[0]
    assert inputted_row.wind_speed is None
    assert inputted_row.wind_direction is None
    assert inputted_row.power_output is None


def test_run_removes_duplicate_entry_rows(spark, dummy_db_paths, seed_table, dummy_day_rows, pipeline_run_date):
    seed_table(dummy_db_paths["bronze"], dummy_day_rows + [dummy_day_rows[0]])  # duplicate an entry

    Curate(spark, pipeline_run_date=pipeline_run_date).run()  # should not raise - logged and deduplicated instead

    silver_df = spark.read.format("delta").load(dummy_db_paths["silver"])
    assert silver_df.count() == 360  # the extra duplicate was dropped, not the whole run halted
