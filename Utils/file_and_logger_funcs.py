from io import TextIOWrapper
from typing import TextIO
import pandas as pd
import sys
import os
import traceback
import re
import json

from loguru import logger


def DeleteFilesInFolder(folder_name: str,
                        except_files: list[str] = []):
    """
    Удаляет все файлы в указанной папке, кроме файлов в списке исключений.

    Args:
      folder_name (str): путь к папке.
      except_files (list[str], optional): список имен файлов, которые нужно исключить из удаления. Defaults to [].
    """

    for file_name in os.listdir(folder_name):
        full_file_name = os.path.join(folder_name, file_name)

        if os.path.isfile(full_file_name) and file_name not in except_files:
            os.remove(full_file_name)


def IsFileInFolder(folder_name: str, file_name: str) -> bool:
    """
    Проверяет, существует ли файл в указанной папке.

    Args:
      file_name: путь к файлу, который нужно проверить.
      folder_name: путь к папке, в которой нужно проверить наличие файла.

    Returns:
      True, если файл существует в папке, в противном случае False.
    """

    full_file_name = os.path.join(folder_name, file_name)
    return os.path.exists(full_file_name)


def CreateFolder(folder_name: str,
                 folder_name_for_log: str = ""):
    """
    Создает папку, использует логирование.
    (в случае исключения также выводит об этом в консоль)

    Args:
        folder_name (str): путь к папке.
        folder_name_for_log (str, optional): имя папки (для логирования). Defaults to "": имя будет идентично folder_name.
    """

    if folder_name_for_log == "":
        folder_name_for_log = folder_name

    try:
        if not os.path.exists(folder_name):
            logger.info(f"Creating folder '{folder_name_for_log}'...".ljust(77))
            os.makedirs(folder_name, exist_ok=True)
            logger.success(f"Creating folder '{folder_name_for_log}': SUCCESS".ljust(77))

    except Exception as exception:
        logger.warning(f"{exception}".ljust(77))


def CombineCSVInFolder(folder_name: str,
                       combined_file_name: str,
                       logger_label: str = "ChEMBL__combine",
                       print_to_console: bool = False,
                       skip_downloaded_files: bool = False):
    """
    Склеивает все .csv файлы в папке в один.

    Args:
        folder_name (str): имя папки с .csv файлами
        combined_file_name (str): имя склеенного .csv файла
        logger_label (str, optional): текст заголовка для логирования. Defaults to "ChEMBL__combine".
        print_to_console (bool, optional): нужно ли выводить логирование в консоль. Defaults to False.
    """

    UpdateLoggerFormat(logger_label, "fg #474747")

    logger.info(f"Start combining all downloads...".ljust(77))
    logger.info(f"{'-' * 77}")

    if IsFileInFolder(folder_name, f"{combined_file_name}.csv") and skip_downloaded_files:
        logger.warning(
            f"File '{combined_file_name}' is in folder, no need to combine".ljust(77))
        return

    combined_df = pd.DataFrame()

    for file_name in os.listdir(folder_name):
        if file_name.endswith('.csv') and file_name != f"{combined_file_name}.csv":

            if print_to_console:
                logger.info(f"Opening '{file_name}'...".ljust(77))

            full_file_name: str = os.path.join(folder_name, file_name)

            if print_to_console:
                logger.success(f"Opening '{file_name}': SUCCESS".ljust(77))

                logger.info(
                    f"Collecting '{file_name}' to pandas.DataFrame()...".ljust(77))
            try:
                df = pd.read_csv(full_file_name, sep=';', low_memory=False)

                if print_to_console:
                    logger.success(
                        f"Collecting '{file_name}' to pandas.DataFrame(): SUCCESS".ljust(77))

                    logger.info(
                        f"Concatenating '{file_name}' to combined_data_frame...".ljust(77))

                combined_df = pd.concat([combined_df, df], ignore_index=True)

                if print_to_console:
                    logger.success(
                        f"Concatenating '{file_name}' to combined_data_frame: SUCCESS".ljust(77))

            except Exception as exception:
                LogException(exception)

            if print_to_console:
                logger.info(f"{'-' * 77}")

    logger.info(
        f"Collecting to combined .csv file in '{folder_name}'...".ljust(77))
    try:
        combined_df.to_csv(
            f"{folder_name}/{combined_file_name}.csv", sep=';', index=False)
        logger.success(
            f"Collecting to combined .csv file in '{folder_name}': SUCCESS".ljust(77))

    except Exception as exception:
        LogException(exception)

    logger.info(f"{'-' * 77}")
    logger.success(f"End combining all downloads".ljust(77))


def UpdateLoggerFormat(logger_label: str,
                       color: str,
                       out: TextIO | TextIOWrapper = sys.stdout):
    """
    Обновляет формат вывода логирования.

    Args:
        logger_label (str): текст заголовка для логирования
        color (str): цвет заголовка для логирования
        out (TextIO | TextIOWrapper, optional): способ вывода. Defaults to sys.stdout.
    """

    logger.remove()
    logger.add(sink=out,
               format="[{time:DD.MM.YYYY HH:mm:ss}]" +
                      f" <{color}>{logger_label}:</{color}>" +
                      " <white>{message}</white>" +
                      " [<level>{level}</level>]")

    with open("logger.json", "w", encoding="utf-8") as file:
        json.dump(
            {
                "logger_label": logger_label,
                "color": color
            },
            file,
            ensure_ascii=False, indent=2
        )


def LogException(exception: Exception,
                 file_name: str = "exceptions.log"):
    """
    Выводит исключение в консоль и записывает в файл.

    Args:
        exception (Exception): исключение.
        file_name (str, optional): имя файла. Defaults to "exceptions.log".
    """

    logger.error(f"{exception}".ljust(77))

    logger_config: dict = {}
    with open("logger.json", "r", encoding="utf-8") as f:
        logger_config = json.load(f)

    with open(file_name, 'a', encoding='utf-8') as f:
        UpdateLoggerFormat(logger_config["logger_label"], logger_config["color"], f)

        logger.error(
            f"{re.sub(r'"(.*?)\",\s+line\s+(\d+)', r'\1:\2', traceback.format_exc())}")
        logger.add(sink=sys.stdout)

    UpdateLoggerFormat(logger_config["logger_label"], logger_config["color"], sys.stdout)
