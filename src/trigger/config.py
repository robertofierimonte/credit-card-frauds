import logging
import sys


class MessageIsNormal(logging.Filter):
    """Filter class for log records that are not errors."""

    def filter(self, record: logging.LogRecord):
        """Return True if log record is not an error, False otherwise.

        Args:
            record (LogRecord): Log record to filter.

        Returns:
            bool: Whether the log is not an error.
        """
        return record.levelname in ["DEBUG", "INFO", "WARNING"]


class MessageIsError(logging.Filter):
    """Filter class for log records that are errors."""

    def filter(self, record):
        """Return True if log record is an error, False otherwise.

        Args:
            record (LogRecord): Log record to filter.

        Returns:
            bool: Whether the log is an error.
        """
        return record.levelname in ["ERROR", "CRITICAL"]


bind = "0.0.0.0:8080"
worker_class = "uvicorn.workers.UvicornWorker"
logconfig_dict = {
    "formatters": {
        "simple": {
            "format": (
                "%(asctime)s | %(levelname)-8s | "
                "%(name)s:%(funcName)s:%(lineno)d - %(message)s"
            )
        }
    },
    "filters": {
        "message_is_normal": {
            "()": "src.trigger.config.MessageIsNormal",
        },
        "message_is_error": {
            "()": "src.trigger.config.MessageIsError",
        },
    },
    "handlers": {
        "console_stdout": {
            "level": "DEBUG",
            "filters": ["message_is_normal"],
            "class": "logging.StreamHandler",
            "formatter": "simple",
            "stream": sys.stdout,
        },
        "console_stderr": {
            "level": "DEBUG",
            "filters": ["message_is_error"],
            "class": "logging.StreamHandler",
            "formatter": "simple",
            "stream": sys.stderr,
        },
    },
    "loggers": {
        "": {
            "handlers": ["console_stdout", "console_stderr"],
            "level": "INFO",
        },
        "gunicorn": {
            "handlers": ["console_stdout"],
            "level": "INFO",
        },
        "gunicorn.access": {
            "handlers": ["console_stdout"],
            "level": "INFO",
        },
        "gunicorn.error": {
            "handlers": ["console_stdout", "console_stderr"],
            "level": "INFO",
        },
    },
    "root": {"level": "INFO", "handlers": ["console_stdout", "console_stderr"]},
}
