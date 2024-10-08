from loguru import logger

import sys


def UpdateLoggerFormat(logger_label: str, color: str) -> None:
    """
    Обновляет формат вывода логирования

    Args:
        logger_label (str): текст заголовка для логирования
        color (str): цвет заголовка для логирования
    """

    logger.remove()
    logger.add(sink=sys.stdout,
               format="[{time:DD.MM.YYYY HH:mm:ss}]" +
                      f" <{color}>{logger_label}:</{color}>" +
                      " <white>{message}</white>" +
                      " [<level>{level}</level>]")
