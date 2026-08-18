from turbine_etl.transformers import Conform


def test_run_writes_conformed_rows_to_gold(spark, dummy_db_paths, seed_table, dummy_day_rows):
    seed_table(dummy_db_paths["silver"], dummy_day_rows)

    Conform(spark).run()

    gold_df = spark.read.format("delta").load(dummy_db_paths["gold"])
    assert gold_df.count() == 360
    assert "measurement_id" in gold_df.columns


def test_run_removes_duplicate_measurement_id_rows(spark, dummy_db_paths, seed_table, dummy_day_rows):
    seed_table(dummy_db_paths["silver"], dummy_day_rows + [dummy_day_rows[0]])  # duplicate row

    Conform(spark).run()  # should not raise - duplicates are logged and removed, not fatal

    gold_df = spark.read.format("delta").load(dummy_db_paths["gold"])
    assert gold_df.count() == 360  # the duplicate was dropped, not the whole run halted


def test_run_keeps_anomalous_rows_and_does_not_raise(spark, dummy_db_paths, seed_table, dummy_day_rows):
    rows = list(dummy_day_rows)
    rows[0] = dict(rows[0], power_output=4.9)  # extreme outlier vs. the uniform 2.5 baseline

    seed_table(dummy_db_paths["silver"], rows)

    Conform(spark).run()  # should not raise - anomalies are logged only, the row is kept

    gold_df = spark.read.format("delta").load(dummy_db_paths["gold"])
    assert gold_df.count() == 360  # the anomalous row was not removed
