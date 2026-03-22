import logging
import sys


def get_logger(name: str) -> logging.Logger:
    """Return a named logger with a consistent stdout format.

    Each module gets its own named logger while sharing a single handler.
    This avoids duplicate log lines if logging is configured elsewhere
    or if the script is reused in a larger application.
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(
            logging.Formatter(
                "%(asctime)s [%(levelname)s] %(name)s — %(message)s",
                datefmt="%Y-%m-%dT%H:%M:%S",
            )
        )
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger
