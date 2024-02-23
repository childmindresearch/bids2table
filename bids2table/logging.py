import logging
import sys
from pathlib import Path
from typing import Dict, Optional, Tuple, Union


class RepetitiveFilter(logging.Filter):
    """
    Suppress similar log messages after a number of repeats.
    """

    def __init__(self, max_repeats: int = 5):
        self.max_repeats = max_repeats

        self._counts: Dict[Tuple[str, int], int] = {}

    def filter(self, record: logging.LogRecord):
        key = record.pathname, record.lineno
        count = self._counts.get(key, 0)
        if count == self.max_repeats:
            record.msg += " [future messages suppressed]"
        self._counts[key] = count + 1
        return count <= self.max_repeats


def setup_logging(
    name: Optional[str] = None,
    level: Optional[Union[int, str]] = None,
    log_path: Optional[Union[str, Path]] = None,
    max_repeats: Optional[int] = 5,
    overwrite: bool = False,
    propagate: bool = False,
) -> logging.Logger:
    """
    Setup a logger in a particular way.
    """
    logger = logging.getLogger(name=name)
    if len(logger.handlers) > 0 and not overwrite:
        return logger

    if level is not None:
        logger.setLevel(level)

    # clean up any pre-existing filters and handlers
    for f in logger.filters:
        logger.removeFilter(f)
    for h in logger.handlers:
        logger.removeHandler(h)
        h.close()

    if max_repeats:
        logger.addFilter(RepetitiveFilter(max_repeats=max_repeats))

    fmt = "[%(levelname)s %(name)s %(asctime)s]: %(message)s"
    formatter = logging.Formatter(fmt, datefmt="%y-%m-%d %H:%M:%S")

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    stream_handler.setLevel(logger.level)
    logger.addHandler(stream_handler)

    if log_path is not None:
        file_handler = logging.FileHandler(log_path, mode="a")
        file_handler.setFormatter(formatter)
        file_handler.setLevel(logger.level)
        logger.addHandler(file_handler)

    if not propagate:
        logger.propagate = False
    return logger
