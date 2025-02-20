import json
from io import TextIOWrapper
import re
import sys
import traceback
from typing import TextIO

from loguru import logger


def UpdateLoggerFormat(logger_label: str,
                       logger_color: str,
                       out: TextIO | TextIOWrapper = sys.stdout,
                       temp_logger_file: str = "logger.json"):
    """
    Обновляет формат вывода логирования.

    Args:
        logger_label (str): текст заголовка для логирования.
        logger_color (str): цвет заголовка для логирования.
        out (TextIO | TextIOWrapper, optional): способ вывода. Defaults to sys.stdout.
        temp_logger_file (str, optional): временный файл записи конфигурации логов. 
                                          Defaults to "logger.json".
    """

    def AlignedFormat(record):
        return "[{time:DD.MM.YYYY HH:mm:ss}] " +\
               f"<{logger_color}>{logger_label}:</{logger_color}> " +\
               f"{record["message"]} ".ljust(78) +\
               f"[<level>{record["level"]}</level>]\n"

    logger.remove()
    logger.add(sink=out,
               format=AlignedFormat)

    with open(temp_logger_file, "w", encoding="utf-8") as file:
        json.dump(
            {
                "logger_label": logger_label,
                "color": logger_color
            },
            file,
            ensure_ascii=False,
            indent=2
        )


def LogException(exception: Exception,
                 file_name: str = "exceptions.log"):
    """
    Выводит исключение в консоль и записывает в файл.

    Args:
        exception (Exception): исключение.
        file_name (str, optional): имя файла. Defaults to "exceptions.log".
    """

    logger.error(f"{exception}")

    logger_config: dict = {}
    with open("logger.json", "r", encoding="utf-8") as f:
        logger_config = json.load(f)

    with open(file_name, "a", encoding="utf-8") as f:
        UpdateLoggerFormat(logger_config["logger_label"],
                           logger_config["color"], f)

        logger.error(
            f"{re.sub(r'"(.*?)\",\s+line\s+(\d+)', r'\1:\2', traceback.format_exc())}")

    UpdateLoggerFormat(logger_config["logger_label"],
                       logger_config["color"], sys.stdout)
