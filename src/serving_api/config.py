import logging
import sys


class MessageIsNormal(logging.Filter):
    """Helper class for gunicorn logging."""

    def filter(self, record: logging.LogRecord) -> bool:
        """Filter records based on their levels.

        Args:
            record (logging.LogRecord): Logger record

        Returns:
            bool: True if the record is not an error, False otherwise
        """
        return record.levelname in ["DEBUG", "INFO", "WARNING"]


class MessageIsError(logging.Filter):
    """Helper class for gunicorn logging."""

    def filter(self, record: logging.LogRecord) -> bool:
        """Filter records based on their levels.

        Args:
            record (logging.LogRecord): Logger record

        Returns:
            bool: True if the record is an error, False otherwise
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
        "require_message_is_normal": {"()": "src.serving_api.config.MessageIsNormal"},
        "require_message_is_error": {"()": "src.serving_api.config.MessageIsError"},
    },
    "handlers": {
        "console_stdout": {
            "level": "DEBUG",
            "filters": ["require_message_is_normal"],
            "class": "logging.StreamHandler",
            "formatter": "simple",
            "stream": sys.stdout,
        },
        "console_stderr": {
            "level": "DEBUG",
            "filters": ["require_message_is_error"],
            "class": "logging.StreamHandler",
            "formatter": "simple",
            "stream": sys.stderr,
        },
    },
    "loggers": {
        "": {"handlers": ["console_stdout", "console_stderr"], "level": "INFO"},
        "gunicorn": {"handlers": ["console_stdout"], "level": "INFO"},
        "gunicorn.access": {"handlers": ["console_stdout"], "level": "INFO"},
        "gunicorn.error": {
            "handlers": ["console_stdout", "console_stderr"],
            "level": "INFO",
        },
        "tensorflow": {
            "handlers": ["console_stdout", "console_stderr"],
            "level": "INFO",
        },
    },
    "root": {"level": "INFO", "handlers": ["console_stdout", "console_stderr"]},
}
