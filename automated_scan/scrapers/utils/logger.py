import logging
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

LOG_DIR = Path(__file__).parent.parent.parent / "logs"

LOG_DIR.mkdir(exist_ok=True)

LOG_FMT = (
    "[%(asctime)s] "
    "%(levelname)-8s "
    "[%(name)s] "
    "%(message)-70s "
    "(%(filename)s:%(lineno)d)"
)

COLORS = {
    "DEBUG": "\033[36m",
    "INFO": "\033[32m",
    "WARNING": "\033[33m",
    "ERROR": "\033[31m",
    "reset": "\033[0m",
}


class ColorFormatter(logging.Formatter):
    def format(self, record) -> str:
        color = COLORS.get(record.levelname, COLORS["reset"])

        levelname = record.levelname

        record.levelname = f"{color}{levelname:<8}{COLORS['reset']}"

        formatted = super().format(record)

        record.levelname = levelname

        return formatted


def get_logger(name: str | None = None) -> logging.Logger:
    if not name:
        name = Path(__file__).stem

    logger = logging.getLogger(name)

    logger.setLevel(logging.INFO)

    if logger.hasHandlers():
        return logger

    formatting = {"fmt": LOG_FMT, "datefmt": "%Y-%m-%d | %H:%M:%S"}

    file_handler = TimedRotatingFileHandler(
        LOG_DIR / "fetch.log",
        when="midnight",
        interval=1,
        backupCount=3,
        encoding="utf-8",
        utc=False,
    )

    file_handler.setFormatter(logging.Formatter(**formatting))

    console_handler = logging.StreamHandler()

    console_handler.setFormatter(ColorFormatter(**formatting))

    logger.addHandler(file_handler)

    logger.addHandler(console_handler)

    logger.propagate = False

    return logger


__all__ = ["get_logger", "ColorFormatter"]
