"""Readable log output for notebooks."""

import logging


def get_logger(name: str = "web_content_kit") -> logging.Logger:
    """Logger that prints ``HH:MM:SS LEVEL message`` lines in the cell output.

    Calling it again (for example when a cell is re-run) does not add duplicate handlers.
    The package's child loggers (``web_content_kit.crawler`` etc.) write through it too.
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)-7s %(message)s", "%H:%M:%S"))
    logger.addHandler(handler)
    logger.propagate = False
    return logger
