from io import TextIOWrapper
import os
import shutil

import pandas as pd

from Utils.verbose_logger import Any, v_logger, LogMode

from Configurations.config import config, Config


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

    for item in os.listdir(folder_name):
        item_path: str = os.path.join(folder_name, item)

        if item not in except_files:
            if os.path.isfile(item_path):
                os.remove(item_path)

            elif delete_folders and os.path.isdir(item_path):
                shutil.rmtree(item_path)


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
                       combined_file_name: str):
    """
    Склеивает все .csv файлы в папке в один.

    Args:
        folder_name (str): имя папки с .csv файлами.
        combined_file_name (str): имя склеенного .csv файла.
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


def SaveMolfilesToSDF(data: pd.DataFrame,
                      file_name: str,
                      molecule_id_column_name: str,
                      extra_data: pd.DataFrame = pd.DataFrame(),
                      indexing_lists: bool = False):
    """
    Сохраняет molfiles из pd.DataFrame в .sdf файл.

    Args:
        data (pd.DataFrame): DataFrame с колонками molfile и [id_column_name].
        file_name (str): имя файла (без ".sdf").
        id_column_name (str): имя колонки, содержащей id соединения
        extra_data (pd.DataFrame, optional): дополнительная информация. 
                                             Defaults to pd.DataFrame().
        indexing_lists (bool, optional): нужно ли индексировать списки, 
                                         если они есть в колонках. Defaults to False.
    """

    def WriteColumnAndValueToSDF(file: TextIOWrapper,
                                 value: Any,
                                 column: str = ""):
        """
        Записывает столбец и значение в .sdf файл.

        Args:
            file (TextIOWrapper): открытый файл для записи.
            value (Any): значение, которое нужно записать.
            column (str, optional): имя столбца. Defaults to "". 
        """

        if not column:
            return

        if isinstance(value, list) or isinstance(value, pd.Series):
            file.write(f"> <{column}>\n")

            i: int = 0
            for elem in value:
                # если value - это список словарей
                if isinstance(elem, dict):
                    WriteColumnAndValueToSDF(file, elem)

                else:
                    elem = str(elem)

                    if elem != "nan" and elem != "None" and elem != "":
                        if indexing_lists:
                            file.write(f"{i}: {elem}\n")
                        else:
                            file.write(f"{elem}\n")
                i += 1

        elif isinstance(value, dict):
            file.write(f"> <{column}>\n")

            for key, elem in value.items():
                elem = str(elem)

                if elem != "nan" and elem != "None" and elem != "":
                    file.write(f"{key}: {elem}\n")

        else:
            value = str(value)

            if value != "nan" and value != "None" and value != "":
                file.write(f"> <{column}>\n")

                file.write(f"{value}\n")

        file.write("\n")

    with open(f"{file_name}.sdf", "w", encoding="utf-8") as f:
        for value in data.values:
            molecule_id, molfile = value

            f.write(f"{molecule_id}{molfile}\n\n")

            if not extra_data.empty:
                df = extra_data.set_index(f"{molecule_id_column_name}")

                for column in df.columns:
                    WriteColumnAndValueToSDF(
                        f, df.loc[molecule_id, column], column)

            f.write("$$$$\n")

            v_logger.info(
                f"Writing {molecule_id} data to .sdf file...",
                LogMode.VERBOSELY)
