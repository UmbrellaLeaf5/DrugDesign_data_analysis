import pandas as pd
from loguru import logger
import json

from Utils.file_and_logger_funcs import *


def PrimaryAnalysisByColumns(data_frame: pd.DataFrame,
                             data_name: str,
                             folder_name: str,
                             logger_label: str = "ChEMBL_analysis",
                             logger_color: str = "fg #C48BC0",
                             print_to_console: bool = False,
                             save_to_csv: bool = True):
    """
    Проводит первичный анализ pd.DataFrame с возможностью вывода в консоль и сохранения в .csv файл.

    Args:
        data_frame (pd.DataFrame): исходный pd.DataFrame.
        data_name (str): имя исходных данных (нужно для логирования).
        folder_name (str): имя папки, куда сохранять .csv файл.
        logger_label (str, optional): текст заголовка логирования. Defaults to "ChEMBL_analysis".
        logger_color (str, optional): цвет заголовка логирования. Defaults to "fg #C48BC0".
        print_to_console (bool, optional): нужно ли выводить информацию в консоль. Defaults to False.
        save_to_csv (bool, optional): нужно ли сохранять информацию в .csv файл. Defaults to True.
    """
    old_logger_config: dict = {}
    with open("logger.json", "r", encoding="utf-8") as f:
        old_logger_config = json.load(f)

    UpdateLoggerFormat(logger_label, logger_color)

    logger.info(f"Start analysis of '{data_name}'...")

    summary: dict[str, list] = {"Column":            [],
                                "Data type":         [],
                                "Non-empty strings": [],
                                "Common value":      [],
                                "Max value":         [],
                                "Min value":         []}

    for column in data_frame.columns:
        # имя столбца
        if print_to_console:
            logger.info("-" * 77)
            logger.info(f"{"Column".ljust(30)}: {column}.")

        if save_to_csv:
            summary["Column"].append(column)

        # тип данных
        try:
            data_type = data_frame[column].dtype

            if print_to_console:
                logger.info(f"{"Type of data".ljust(30)}: {data_type}.")

            if save_to_csv:
                summary["Data type"].append(data_type)

        except Exception as exception:
            if print_to_console:
                logger.warning(
                    f"{"Data type: EXCEPTION".ljust(30)}: {exception}.")

            if save_to_csv:
                summary["Data type"].append("")

        # количество ненулевых строк
        non_null_count = 0
        for value in data_frame[column]:
            if value:
                non_null_count += 1

        if print_to_console:
            logger.info(f"{"Non-empty strings".ljust(30)}: {non_null_count}.")

        if save_to_csv:
            summary["Non-empty strings"].append(non_null_count)

        # наиболее часто встречающееся значение
        try:
            mode_values = data_frame[column].mode()
            if len(mode_values) > 0:
                common_value = mode_values[0]

            else:
                common_value = ""

            if print_to_console:
                logger.info(f"{"Common value".ljust(30)}: {common_value}.")

            if save_to_csv:
                summary["Common value"].append(common_value)

        except Exception as exception:
            if print_to_console:
                logger.warning(
                    f"{"Common value: EXCEPTION".ljust(30)}: {exception}.")

            if save_to_csv:
                summary["Common value"].append("")

        # максимальное и минимальное значения
        try:
            try:
                max_value = data_frame[column].max()
                min_value = data_frame[column].min()

            except TypeError:
                max_value = None
                min_value = None

                for value in data_frame[column]:
                    if value is None:
                        continue

                    elif isinstance(value, (list, str)):
                        if max_value is None or len(value) > len(max_value):
                            max_value = value
                        if min_value is None or len(value) < len(min_value):
                            min_value = value

            if print_to_console:
                logger.info(f"{"Max value".ljust(30)}: {max_value}.")
                logger.info(f"{"Min value".ljust(30)}: {min_value}.")

            if save_to_csv:
                summary["Max value"].append(max_value)
                summary["Min value"].append(min_value)

        except Exception as exception:
            if print_to_console:
                logger.warning(f"{"Max value: EXCEPTION".ljust(30)}: {exception}.")
                logger.warning(f"{"Min value: EXCEPTION".ljust(30)}: {exception}.")

            if save_to_csv:
                summary["Max value"].append("")
                summary["Min value"].append("")

    if save_to_csv:
        logger.info("Saving primary analysis to .csv file...")

        file_name: str = f"{folder_name}/{data_name}_analysis.csv"

        pd.DataFrame(summary).to_csv(file_name, sep=";", index=False)

        logger.success("Saving primary analysis to .csv file!")

    logger.success(f"End analysis of '{data_name}'!")

    UpdateLoggerFormat(old_logger_config["logger_label"],
                       old_logger_config["color"])
