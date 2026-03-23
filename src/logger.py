import logging
import sys


def get_logger(name: str) -> logging.Logger:
    """Return a named logger with a consistent format.

    INFO goes to stdout, ERROR and above to stderr.
    propagate=False prevents duplicate lines from the root logger.
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        fmt = logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s — %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S",
        )

        # INFO/WARNING : stdout
        info_handler = logging.StreamHandler(sys.stdout)
        info_handler.setFormatter(fmt)
        info_handler.setLevel(logging.INFO)
        info_handler.addFilter(lambda record: record.levelno < logging.ERROR)

        # ERROR/CRITICAL : stderr (separate stream from info and warnings for easier log routing)
        error_handler = logging.StreamHandler(sys.stderr)
        error_handler.setFormatter(fmt)
        error_handler.setLevel(logging.ERROR)

        logger.addHandler(info_handler)
        logger.addHandler(error_handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger
