import logging
import os
from datetime import datetime

LOG_DIR = os.path.join("etl_outputs", "etl_history")


class _WarningTrackingHandler(logging.Handler):
    """Remembers whether any WARNING-or-above record has been logged, so run_etl.py can report
    an accurate 'warnings occurred' flag without every warning call site updating one by hand."""

    def __init__(self):
        super().__init__(level=logging.WARNING)
        self.triggered = False

    def emit(self, record: logging.LogRecord) -> None:
        self.triggered = True


_warning_tracker = _WarningTrackingHandler()


def get_logger(name: str = "turbine_etl") -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)

    os.makedirs(LOG_DIR, exist_ok=True)
    log_file = os.path.join(LOG_DIR, f"{datetime.now():%Y%m%d_%H%M%S}.log")
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s - %(message)s"))
    logger.addHandler(file_handler)
    logger.addHandler(_warning_tracker)

    return logger


def had_warnings() -> bool:
    return _warning_tracker.triggered
