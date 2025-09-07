import logging
import sys

# Базовая конфигурация логирования для всей библиотеки
LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def setup_logging(level: int = logging.INFO) -> logging.Logger:
    """
    Настройка логирования и получение логгера.

    :param level: уровень логирования (по умолчанию INFO)
    :return: объект logging.Logger
    """
    logger = logging.getLogger("max_notifier")
    logger.setLevel(level)

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(level)
        formatter = logging.Formatter(LOG_FORMAT, datefmt=DATE_FORMAT)
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger


logger = setup_logging()  # создаём и настраиваем логгер
