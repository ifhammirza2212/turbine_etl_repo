from pyspark.sql.types import FloatType, IntegerType, StringType, TimestampType

# Schema definitions using Spark StructType

PRE_CONFORMED_SCHEMA = {  # Applicable to both raw and curated tables
    "timestamp": {"dtype": TimestampType(), "nullable": False, "range": ("2020-01-01", None)},
    "turbine_id": {"dtype": IntegerType(), "nullable": False, "range": (1, 15)},
    "wind_speed": {"dtype": FloatType(), "nullable": True, "range": (0, 20)},
    "wind_direction": {"dtype": IntegerType(), "nullable": True, "range": (0, 360)},
    "power_output": {"dtype": FloatType(), "nullable": True, "range": (0, 5)},
}

TURBINE_CONFORMED_SCHEMA = {
    "measurement_id": {"dtype": StringType(), "nullable": False, "range": (None, 100)},
    "measurement_timestamp": {"dtype": TimestampType(), "nullable": False, "range": ("2020-01-01", None)},
    "turbine_id": {"dtype": IntegerType(), "nullable": False, "range": (1, 15)},
    "wind_speed": {"dtype": FloatType(), "nullable": True, "range": (0, 20)},
    "wind_direction": {"dtype": IntegerType(), "nullable": True, "range": (0, 360)},
    "power_output": {"dtype": FloatType(), "nullable": True, "range": (0, 5)},
    "pipeline_run_timestamp": {"dtype": TimestampType(), "nullable": False, "range": None},
}

SUMMARY_STATS_SCHEMA = {
    "turbine_id": {"dtype": IntegerType(), "nullable": False, "range": (1, 15)},
    "min_power_output": {"dtype": FloatType(), "nullable": False, "range": (0, 5)},
    "max_power_output": {"dtype": FloatType(), "nullable": False, "range": (0, 5)},
    "avg_power_output": {"dtype": FloatType(), "nullable": False, "range": (0, 5)},
    "calculation_date": {"dtype": TimestampType(), "nullable": False, "range": None},
}
