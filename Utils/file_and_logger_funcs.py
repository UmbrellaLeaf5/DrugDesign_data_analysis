import json
from io import TextIOWrapper
import os
import pandas as pd
import re
import shutil
import sys
import traceback
from typing import TextIO

from loguru import logger


def DeleteFilesInFolder(folder_name: str,
                        except_files: list[str] = [],
                        delete_folders: bool = False):
    """
    Удаляет все файлы в указанной папке, кроме файлов в списке исключений.

    Args:
      folder_name (str): путь к папке.
      except_files (list[str], optional): список имен файлов, которые нужно исключить из удаления. Defaults to [].
      delete_folders (bool, optional): удалять ли вложенные папки. Defaults to False.
    """

    for file_name in os.listdir(folder_name):
        full_file_name = os.path.join(folder_name, file_name)

        if os.path.isfile(full_file_name) and file_name not in except_files:
            os.remove(full_file_name)

        elif os.path.isdir(full_file_name) and delete_folders:
            shutil.rmtree(full_file_name)


def IsFileInFolder(file_name: str, folder_name: str) -> bool:
    """
    Проверяет, существует ли файл в указанной папке.

    Args:
      file_name: имя файла, который нужно проверить.
      folder_name: путь к папке, в которой нужно проверить наличие файла.

    Returns:
      True, если файл существует в папке, в противном случае False.
    """

    full_file_name = os.path.join(folder_name, file_name)
    return os.path.exists(full_file_name)


def IsFolderEmpty(folder_name: str) -> bool:
    """
    Проверяет, является ли папка пустой.

    Args:
        folder_name: путь к папке, которую нужно проверить.

    Returns:
        True, если папка существует и пуста, иначе False.
    """
    try:
        return len(os.listdir(folder_name)) == 0

    except FileNotFoundError:
        return False


def CreateFolder(folder_name: str,
                 folder_name_for_log: str = ""):
    """
    Создает папку, использует логирование.

    Args:
        folder_name (str): путь к папке.
        folder_name_for_log (str, optional): имя папки (для логирования). Defaults to "": имя будет идентично folder_name.
    """

    if folder_name_for_log == "":
        folder_name_for_log = folder_name

    if not os.path.exists(folder_name):
        logger.info(f"Creating folder '{folder_name_for_log}'...")

        os.makedirs(folder_name, exist_ok=True)

        logger.success(f"Creating folder '{folder_name_for_log}'!")


def MoveFileToFolder(file_name: str, curr_folder_name: str,
                     folder_name: str):
    """
    Перемещает файл в указанную папку.

    Args:
        file_name (str): имя файла, который нужно переместить.
        curr_folder_name (str): путь к папке, содержащей файл.
        folder_name (str): путь к целевой папке, куда будет перемещен файл.
    """

    os.makedirs(folder_name, exist_ok=True)

    full_file_name = os.path.join(curr_folder_name, file_name)

    # если такой уже существует
    if os.path.exists(os.path.join(folder_name, file_name)):
        os.remove(os.path.join(folder_name, file_name))

    shutil.move(full_file_name, folder_name)


def CombineCSVInFolder(folder_name: str,
                       combined_file_name: str,
                       separator: str = ";",
                       logger_label: str = "ChEMBL__combine",
                       logger_color: str = "fg #474747",
                       print_to_console: bool = False,
                       skip_downloaded_files: bool = False):
    """
    Склеивает все .csv файлы в папке в один.

    Args:
        folder_name (str): имя папки с .csv файлами.
        combined_file_name (str): имя склеенного .csv файла.
        sep (str, optional): разделитель между колонками внутри .csv файлов. Defaults to ";".
        logger_label (str, optional): текст заголовка для логирования. Defaults to "ChEMBL__combine".
        print_to_console (bool, optional): нужно ли выводить логирование в консоль. Defaults to False.
        skip_downloaded_files (bool, optional): Пропускать ли уже скачанные файлы. Defaults to False.
    """

    old_logger_config: dict = {}
    with open("logger.json", "r", encoding="utf-8") as f:
        old_logger_config = json.load(f)

    UpdateLoggerFormat(logger_label, logger_color)

    logger.info("Start combining all downloads...")
    logger.info(f"{'-' * 77}")

    if IsFileInFolder(folder_name, f"{combined_file_name}.csv") and skip_downloaded_files:
        logger.warning(
            f"File '{combined_file_name}' is in folder, no need to combine.")

        UpdateLoggerFormat(old_logger_config["logger_label"],
                           old_logger_config["color"])
        return

    combined_df = pd.DataFrame()

    for file_name in os.listdir(folder_name):
        if file_name.endswith('.csv') and file_name != f"{combined_file_name}.csv":
            full_file_name: str = os.path.join(folder_name, file_name)

            if print_to_console:
                logger.info(f"Collecting '{file_name}' to pandas.DataFrame()...")

            df = pd.read_csv(full_file_name, sep=separator, low_memory=False)

            if print_to_console:
                logger.success(f"Collecting '{file_name}' to pandas.DataFrame()!")

                logger.info(f"Concatenating '{file_name}' to combined_data_frame...")

            combined_df = pd.concat([combined_df, df], ignore_index=True)

            if print_to_console:
                logger.success(
                    f"Concatenating '{file_name}' to combined_data_frame!")

                logger.info(f"{'-' * 77}")

    logger.info(f"Collecting to combined .csv file in '{folder_name}'...")

    combined_df.to_csv(
        f"{folder_name}/{combined_file_name}.csv", sep=separator, index=False)

    logger.success(f"Collecting to combined .csv file in '{folder_name}'!")

    logger.info(f"{'-' * 77}")
    logger.success(f"End combining all downloads!")

    UpdateLoggerFormat(old_logger_config["logger_label"],
                       old_logger_config["color"])


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
        temp_logger_file (str, optional): временный файл записи конфигурации логов. Defaults to "logger.json".
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
