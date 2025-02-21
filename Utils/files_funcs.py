import json
import os
import shutil

import pandas as pd

from Utils.verbose_logger import v_logger, LogMode

from Configurations.config import Config


def DeleteFilesInFolder(folder_name: str,
                        except_files: list[str] = [],
                        delete_folders: bool = False):
    """
    Удаляет все файлы в указанной папке, кроме файлов в списке исключений.

    Args:
      folder_name (str): путь к папке.
      except_files (list[str], optional): список имен файлов, 
                                          которые нужно исключить из удаления. 
                                          Defaults to [].
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
        return True


def MoveFileToFolder(file_name: str,
                     curr_folder_name: str,
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
                       config: Config):
    """
    Склеивает все .csv файлы в папке в один.

    Args:
        folder_name (str): имя папки с .csv файлами.
        combined_file_name (str): имя склеенного .csv файла.
        config (Config): словарь, содержащий параметры конфигурации для процесса скачивания.
    """

    combine_config: Config = config["Utils"]["CombineCSVInFolder"]

    restore_index: int = v_logger.UpdateFormat(combine_config["logger_label"],
                                               combine_config["logger_color"]) - 1

    v_logger.info("Start combining downloads...")
    v_logger.info(f"{'-' * 77}", LogMode.VERBOSELY)

    if IsFileInFolder(folder_name, f"{combined_file_name}.csv") and\
            config["skip_downloaded"]:
        v_logger.info(
            f"File '{combined_file_name}' is in folder, no need to combine.",
            LogMode.VERBOSELY)

        v_logger.RestoreFormat(restore_index)
        return

    combined_df = pd.DataFrame()

    for file_name in os.listdir(folder_name):
        if file_name.endswith('.csv') and file_name != f"{combined_file_name}.csv":
            full_file_name: str = os.path.join(folder_name, file_name)

            v_logger.info(f"Collecting '{file_name}' to pandas.DataFrame...",
                          LogMode.VERBOSELY)

            df = pd.read_csv(full_file_name, sep=config["csv_separator"], low_memory=False)

            v_logger.success(f"Collecting '{file_name}' to pandas.DataFrame!",
                             LogMode.VERBOSELY)
            v_logger.info(
                f"Concatenating '{file_name}' to combined_data_frame...",
                LogMode.VERBOSELY)

            combined_df = pd.concat([combined_df, df], ignore_index=True)

            v_logger.success(
                f"Concatenating '{file_name}' to combined_data_frame!",
                LogMode.VERBOSELY)
            v_logger.info(f"{'-' * 77}", LogMode.VERBOSELY)

    v_logger.info(f"Collecting to combined .csv file in '{folder_name}'...",
                  LogMode.VERBOSELY)

    combined_df.to_csv(
        f"{folder_name}/{combined_file_name}.csv",
        sep=config["csv_separator"], index=False)

    v_logger.success(f"Collecting to combined .csv file in '{folder_name}'!",
                     LogMode.VERBOSELY)
    v_logger.info(f"{'-' * 77}", LogMode.VERBOSELY)
    v_logger.success(f"End combining downloads!")

    v_logger.RestoreFormat(restore_index)
