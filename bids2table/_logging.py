import logging


class RepetitiveFilter(logging.Filter):
    """Suppress similar log messages after a number of repeats."""

    def __init__(self, max_repeats: int = 2):
        self.max_repeats = max_repeats
        self._counts: dict[tuple[str, int, str], int] = {}

    def filter(self, record: logging.LogRecord):
        # Exact repeat of path, line, message.
        key = record.pathname, record.lineno, record.msg
        count = self._counts.get(key, 0) + 1
        if count == self.max_repeats:
            record.msg += " [future messages suppressed]"
        self._counts[key] = count
        return count <= self.max_repeats


def setup_logger(
    name: str | None = None,
    level: int | str | None = None,
    max_repeats: int | None = 2,
    overwrite: bool = False,
) -> logging.Logger:
    """Get logger with my preferred formatting and repetition filtering."""
    logger = logging.getLogger(name)
    if level is not None:
        logger.setLevel(level)

    if logger.handlers and not overwrite:
        return logger

    # clean up any pre-existing filters and handlers
    for f in logger.filters:
        logger.removeFilter(f)
    for h in logger.handlers:
        logger.removeHandler(h)
        h.close()

    if max_repeats:
        logger.addFilter(RepetitiveFilter(max_repeats))

    fmt = "[%(levelname)s %(name)s]: %(message)s"
    formatter = logging.Formatter(fmt)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    return logger
