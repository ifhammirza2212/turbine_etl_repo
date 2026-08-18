"""
When a data quality check fails, a specific, customised error type is raised.
All of these customised error types are defined as class objects here.
"""


class _TurbineETLError(Exception):
    """Common constructor for all turbine_etl errors.

    Accepts either a plain message (existing call sites) or keyword context
    matching message_template's placeholders, which gets auto-formatted.
    """

    log_level = "ERROR"
    handling_method = "raise"
    message_template = "{message}"

    def __init__(self, *args, **context):
        if context:
            args = (self.message_template.format(**context),) + args
        super().__init__(*args)
        self.context = context


class SchemaValidationError(_TurbineETLError):
    """Base class for schema validation errors. Fatal - halts the pipeline."""

    log_level = "ERROR"
    handling_method = "raise"


class FieldNameSchemaError(SchemaValidationError):
    """Raised when a dataframe's fields do not match the expected schema fields."""

    message_template = "incorrect field name {field_name} – data cannot be processed"


class DataTypeSchemaError(SchemaValidationError):
    """Raised when a field contains values that cannot be cast to the expected datatype."""

    message_template = "incorrect datatype value {value} found in {field_name} field – data cannot be processed"


class DataRangeSchemaError(SchemaValidationError):
    """Raised when a field contains values outside its expected range."""

    message_template = "value {value} for {field_name} appears to be out of range – data cannot be processed"


class DataQualityError(_TurbineETLError):
    """Base class for post-ingestion data-quality issues. Logged as warnings; pipeline continues."""

    log_level = "WARNING"
    handling_method = "removal"


class MissingEntryError(DataQualityError):
    """Raised when an expected (turbine_id, timestamp) entry is missing.

    Handling: input a record with the correct timestamp and turbine_id, and NULL for
    wind_speed, wind_direction and power_output.
    """

    handling_method = "input"
    message_template = "missing datapoint due to sensor malfunction for turbine {turbine_id}, at {timestamp}"


class IncorrectTimestampEntryError(DataQualityError):
    """Raised when an entry's timestamp date does not match the expected date.

    Handling: remove the record from the dataset.
    """

    message_template = "incorrect timestamp entry {timestamp}. Expected date in timestamp field is {today}"


class UnexpectedEntryError(DataQualityError):
    """Raised when an entry falls outside the expected (turbine_id, timestamp) set.

    Handling: remove the record from the dataset.
    """

    message_template = "unexpected entry for turbine {turbine_id} at {timestamp}, outside the expected range of entries for {today}"


class DuplicateEntryError(DataQualityError):
    """Raised when more than one record shares the same measurement_id.

    Handling: remove the latest of the duplicate records from the conformed table.
    """

    message_template = "duplicate entry for turbine {turbine_id} at {timestamp}, latest entry rendered invalid"


class AnomalousEntryError(DataQualityError):
    """Raised when power_output values fall outside 2 standard deviations from the per-turbine mean.

    Handling: log only. The record is kept in the conformed table, since it may reflect a
    genuinely dysfunctional turbine that needs investigating rather than bad data.
    """

    handling_method = "raise_only"
    message_template = "{count} anomalous entries for turbine {turbine_id} at {timestamp} omitted from dataset"
