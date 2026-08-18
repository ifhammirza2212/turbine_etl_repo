from turbine_etl.transformers import Conform, Curate, StatsSummary


def test_run_produces_15_row_summary_with_correct_aggregates(
    spark, dummy_db_paths, seed_table, dummy_day_rows, pipeline_run_date
):
    seed_table(dummy_db_paths["bronze"], dummy_day_rows)
    Curate(spark, pipeline_run_date=pipeline_run_date).run()
    Conform(spark).run()

    StatsSummary(spark, pipeline_run_date=pipeline_run_date).run()

    summary_df = spark.read.format("delta").load(dummy_db_paths["summary"])
    rows = summary_df.collect()

    assert len(rows) == 15
    assert all(row.min_power_output == row.max_power_output == row.avg_power_output == 2.5 for row in rows)
