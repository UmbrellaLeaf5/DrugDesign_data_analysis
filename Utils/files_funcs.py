"""
Utils/files_funcs.py

Этот модуль содержит набор функций для работы с файлами и директориями,
включая удаление, перемещение, объединение файлов, а также сохранение
молекулярных структур в формате SDF.
"""

from io import TextIOWrapper
import os
import shutil

import pandas as pd

from Utils.verbose_logger import Any, v_logger, LogMode

from Configurations.config import config, Config


def DeleteFilesInFolder(folder_name: str,
                        except_items: list[str] = [],
                        delete_folders: bool = False):
    """
    Удаляет все файлы в указанной папке, кроме файлов в списке исключений.

    Args:
        folder_name (str): путь к папке.
        except_items (list[str], optional): список исключений (файлов или папок),
                                            которые не нужно удалять.
                                            Defaults to [].
        delete_folders (bool, optional): удалять ли вложенные папки.
                                         Defaults to False.
    """

    # итерируемся по элементам в папке.
    for item in os.listdir(folder_name):
        # формируем путь к текущему элементу.
        item_path: str = os.path.join(folder_name, item)

        # если элемент не в списке исключений.
        if item not in except_items:
            # если это файл, удаляем его.
            if os.path.isfile(item_path):
                os.remove(item_path)

            # если нужно удалять папки и это папка, удаляем её.
            elif delete_folders and os.path.isdir(item_path):
                shutil.rmtree(item_path)


def IsFileInFolder(file_name: str, folder_name: str) -> bool:
    """
    Проверяет, существует ли файл в указанной папке.

    Args:
        file_name (str): имя файла для проверки.
        folder_name (str): путь к папке.

    Returns:
        bool: True, если файл существует, иначе False.
    """

    # формируем полный путь к файлу.
    full_file_name = os.path.join(folder_name, file_name)
    # возвращаем True, если файл существует, иначе False.
    return os.path.exists(full_file_name)


def IsFolderEmpty(folder_name: str) -> bool:
    """
    Проверяет, является ли папка пустой.

    Args:
        folder_name (str): путь к папке.

    Returns:
        bool: True, если папка пуста, иначе False.
    """

    try:
        # если количество элементов в папке равно нулю, возвращаем True.
        return len(os.listdir(folder_name)) == 0

    # если папка не найдена, возвращаем True.
    except FileNotFoundError:
        return True


def MoveFileToFolder(file_name: str,
                     curr_folder_name: str,
                     folder_name: str):
    """
    Перемещает файл в указанную папку.

    Args:
        file_name (str): имя файла.
        curr_folder_name (str): путь к текущей папке файла.
        folder_name (str): путь к целевой папке.
    """

    # создаем целевую папку, если она не существует.
    os.makedirs(folder_name, exist_ok=True)

    # формируем полный путь к файлу.
    full_file_name = os.path.join(curr_folder_name, file_name)
    # формируем полный путь к файлу в целевой папке.
    next_file_name = os.path.join(folder_name, file_name)

    # если файл с таким именем уже существует в целевой папке, удаляем его.
    if os.path.exists(next_file_name):
        os.remove(next_file_name)

    # если файл существует, перемещаем его.
    if os.path.exists(full_file_name):
        shutil.move(full_file_name, folder_name)

    # если файл не существует, логируем предупреждение.
    else:
        v_logger.warning(f"{full_file_name} does not exist!",
                         LogMode.VERBOSELY)


def CombineCSVInFolder(folder_name: str,
                       combined_file_name: str):
    """
    Склеивает все .csv файлы в папке в один.

    Args:
        folder_name (str): имя папки с .csv файлами.
        combined_file_name (str): имя склеенного .csv файла.
    """

    # получаем конфигурацию для объединения CSV-файлов.
    combine_config: Config = config["Utils"]["CombineCSVInFolder"]

    # получаем индекс формата логгера.
    restore_index: int = v_logger.UpdateFormat(
        combine_config["logger_label"], combine_config["logger_color"]) - 1

    v_logger.info("Start combining downloads...")
    v_logger.info(f"{'-' * 77}", LogMode.VERBOSELY)

    # если файл уже существует и нужно пропускать скачанные, выходим.
    if IsFileInFolder(folder_name, f"{combined_file_name}.csv") and\
            config["skip_downloaded"]:
        v_logger.info(
            f"File '{combined_file_name}.csv' is in folder, no need to "
            f"combine.",
            LogMode.VERBOSELY)

        # восстанавливаем формат логгера.
        v_logger.RestoreFormat(restore_index)
        return

    # создаем пустой DataFrame для объединения.
    combined_df = pd.DataFrame()

    # если папка пуста, выходим.
    if len(os.listdir(folder_name)) == 0:
        v_logger.info(f"{folder_name} is empty, no need to combine.")

        # восстанавливаем формат логгера.
        v_logger.RestoreFormat(restore_index)
        return

    # итерируемся по файлам в папке.
    for file_name in os.listdir(folder_name):
        # проверяем, является ли файл CSV-файлом и не является ли он
        # результирующим.
        if file_name.endswith('.csv') and file_name != \
                f"{combined_file_name}.csv":
            # формируем полный путь к файлу.
            full_file_name: str = os.path.join(folder_name, file_name)

            v_logger.info(
                f"Collecting '{file_name}' to pandas.DataFrame...",
                LogMode.VERBOSELY)

            # читаем CSV-файл в DataFrame.
            df = pd.read_csv(full_file_name, sep=config["csv_separator"],
                             low_memory=False)

            v_logger.success(
                f"Collecting '{file_name}' to pandas.DataFrame!",
                LogMode.VERBOSELY)
            v_logger.info(
                f"Concatenating '{file_name}' to combined_data_frame...",
                LogMode.VERBOSELY)

            # объединяем DataFrame с общим DataFrame.
            combined_df = pd.concat([combined_df, df], ignore_index=True)

            v_logger.success(
                f"Concatenating '{file_name}' to combined_data_frame!",
                LogMode.VERBOSELY)
            v_logger.info(f"{'-' * 77}", LogMode.VERBOSELY)

    v_logger.info(f"Collecting to combined .csv file in '{folder_name}'...",
                  LogMode.VERBOSELY)

    # сохраняем объединенный DataFrame в CSV-файл.
    combined_df.to_csv(
        f"{folder_name}/{combined_file_name}.csv",
        sep=config["csv_separator"], index=False)

    v_logger.success(f"Collecting to combined .csv file in '{folder_name}'!",
                     LogMode.VERBOSELY)
    v_logger.info(f"{'-' * 77}", LogMode.VERBOSELY)
    v_logger.success("End combining downloads!")

    # восстанавливаем формат логгера.
    v_logger.RestoreFormat(restore_index)


def SaveMolfilesToSDF(data: pd.DataFrame,
                      file_name: str,
                      molecule_id_column_name: str,
                      extra_data: pd.DataFrame = pd.DataFrame(),
                      indexing_lists: bool = False):
    """
    Сохраняет molfiles из pd.DataFrame в .sdf файл.

    Args:
        data (pd.DataFrame): DataFrame с колонками molfile и id.
        file_name (str): имя файла (без ".sdf").
        molecule_id_column_name (str): имя колонки с id соединения.
        extra_data (pd.DataFrame, optional): дополнительная информация.
                                             Defaults to pd.DataFrame().
        indexing_lists (bool, optional): нужно ли индексировать списки.
                                         Defaults to False.
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

        # если столбец не задан, выходим.
        if not column:
            return

        # если значение - список или pd.Series.
        if isinstance(value, list) or isinstance(value, pd.Series):
            file.write(f"> <{column}>\n")

            i: int = 0
            # итерируемся по элементам списка.
            for elem in value:
                # если элемент - словарь.
                if isinstance(elem, dict):
                    # рекурсивно вызываем функцию для записи словаря.
                    WriteColumnAndValueToSDF(file, elem)

                # если элемент - не словарь.
                else:
                    # преобразуем элемент в строку.
                    elem = str(elem)

                    # если элемент не пустой, записываем его.
                    if elem != "nan" and elem != "None" and elem != "":
                        # если нужно индексировать списки.
                        if indexing_lists:
                            file.write(f"{i}: {elem}\n")
                        # если не нужно индексировать списки.
                        else:
                            file.write(f"{elem}\n")
                i += 1

        # если значение - словарь.
        elif isinstance(value, dict):
            file.write(f"> <{column}>\n")

            # итерируемся по элементам словаря.
            for key, elem in value.items():
                # преобразуем элемент в строку.
                elem = str(elem)

                # если элемент не пустой, записываем его.
                if elem != "nan" and elem != "None" and elem != "":
                    file.write(f"{key}: {elem}\n")

        # если значение - не список и не словарь.
        else:
            # преобразуем значение в строку.
            value = str(value)

            # если значение не пустое, записываем его.
            if value != "nan" and value != "None" and value != "":
                file.write(f"> <{column}>\n")

                file.write(f"{value}\n")

        # записываем пустую строку для разделения значений.
        file.write("\n")

    # открываем файл для записи.
    with open(f"{file_name}.sdf", "w", encoding="utf-8") as f:
        # итерируемся по строкам DataFrame.
        for value in data.values:
            # получаем id молекулы и molfile.
            molecule_id, molfile = value

            # записываем id молекулы и molfile в файл.
            f.write(f"{molecule_id}{molfile}\n\n")

            # если есть дополнительная информация.
            if not extra_data.empty:
                # устанавливаем id молекулы в качестве индекса.
                df = extra_data.set_index(f"{molecule_id_column_name}")

                # итерируемся по столбцам DataFrame.
                for column in df.columns:
                    # записываем столбец и значение в файл.
                    WriteColumnAndValueToSDF(
                        f, df.loc[molecule_id, column], column)

            # записываем разделитель между молекулами.
            f.write("$$$$\n")

            v_logger.info(
                f"Writing {molecule_id} data to .sdf file...",
                LogMode.VERBOSELY)
