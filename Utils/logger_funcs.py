from loguru import logger

import sys


def LoggerFormatUpdate(logger_label: str, color: str) -> None:

    logger.remove()
    logger.add(sink=sys.stdout,
               format="[{time:DD.MM.YYYY HH:mm:ss}]" +
                      f" <{color}>{logger_label}:</{color}>" +
                      " <white>{message}</white>" +
                      " [<level>{level}</level>]")
