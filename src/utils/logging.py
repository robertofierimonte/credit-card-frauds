import logging

from google.cloud.logging_v2.handlers import StructuredLogHandler
from loguru import logger


def setup_logger(level: int = logging.DEBUG) -> None:
    """Set up Loguru to work with GCP.

    Args:
        level (int, optional): Minimum log level to print. Defaults to logging.DEBUG.
    """
    logger.remove()
    handler = StructuredLogHandler()
    handler.setLevel(level=level)
    logger.add(handler)
