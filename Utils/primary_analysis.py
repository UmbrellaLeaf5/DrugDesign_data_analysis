# type: ignore

import pandas as pd

try:
    from file_and_logger_funcs import *

except ImportError:
    from Utils.file_and_logger_funcs import *


def DataAnalysisByColumns(data_frame: pd.DataFrame,
                          data_name: str,
                          folder_name: str,
                          logger_label: str = "ChEMBL_analysis",
                          should_print_to_console: bool = False,
                          should_save_to_csv: bool = True) -> None:
    """
    Проводит первичный анализ pd.DataFrame с возможностью вывода в консоль и сохранения в .csv файл

    Args:
        data_frame (pd.DataFrame): исходный pd.DataFrame
        data_name (str): имя исходных данных (нужно для логирования)
        folder_name (str): имя папки, куда сохранять .csv файл
        logger_label (str, optional): текст заголовка логирования. Defaults to "ChEMBL_analysis".
        should_print_to_console (bool, optional): нужно ли выводить информацию в консоль. Defaults to False.
        should_save_to_csv (bool, optional): нужно ли сохранять информацию в .csv файл. Defaults to True.
    """

    UpdateLoggerFormat(logger_label, "magenta")

    logger.info(f"Start analysis of '{data_name}'...".ljust(77))

    summary: dict = {'Column':            [],
                     'Data type':         [],
                     'Non-empty strings': [],
                     'Common value':      [],
                     'Max value':         [],
                     'Min value':         []}

    for column in data_frame.columns:
        # имя столбца
        if should_print_to_console:
            logger.info("-" * 85)
            logger.info(f"{"Column".ljust(30)}: {column}".ljust(77))

        if should_save_to_csv:
            summary['Column'].append(column)

        # тип данных
        try:
            data_type = data_frame[column].dtype

            if should_print_to_console:
                logger.info(f"{"Type of data".ljust(30)}: {
                            data_type}".ljust(77))

            if should_save_to_csv:
                summary['Data type'].append(data_type)

        except Exception as exception:
            if should_print_to_console:
                logger.warning(
                    f"{"Data type:EXCEPTION".ljust(30)}: {exception}".ljust(77))

            if should_save_to_csv:
                summary['Data type'].append("")

        # количество ненулевых строк
        non_null_count = 0
        for value in data_frame[column]:
            if value:
                non_null_count += 1

        if should_print_to_console:
            logger.info(f"{"Non-empty strings".ljust(30)
                           }: {non_null_count}".ljust(77))

        if should_save_to_csv:
            summary['Non-empty strings'].append(non_null_count)

        # наиболее часто встречающееся значение
        try:
            mode_values = data_frame[column].mode()
            if len(mode_values) > 0:
                common_value = mode_values[0]

            else:
                common_value = ""

            if should_print_to_console:
                logger.info(f"{"Common value".ljust(30)}: {
                            common_value}".ljust(77))

            if should_save_to_csv:
                summary['Common value'].append(common_value)

        except Exception as exception:
            if should_print_to_console:
                logger.warning(
                    f"{"Common value:EXCEPTION".ljust(30)}: {exception}".ljust(77))

            if should_save_to_csv:
                summary['Common value'].append("")

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

            if should_print_to_console:
                logger.info(f"{"Max value".ljust(30)}: {max_value}".ljust(77))
                logger.info(f"{"Min value".ljust(30)}: {min_value}".ljust(77))

            if should_save_to_csv:
                summary['Max value'].append(max_value)
                summary['Min value'].append(min_value)

        except Exception as exception:
            if should_print_to_console:
                logger.warning(
                    f"{"Max value:EXCEPTION".ljust(30)}: {exception}".ljust(77))
                logger.warning(
                    f"{"Min value:EXCEPTION".ljust(30)}: {exception}".ljust(77))

            if should_save_to_csv:
                summary['Max value'].append("")
                summary['Min value'].append("")

    if should_save_to_csv:
        try:
            logger.info(
                "Saving primary analysis to .csv file...".ljust(77))

            file_name: str = f"{folder_name}/{data_name}_analysis.csv"

            pd.DataFrame(summary).to_csv(file_name, index=False)

            logger.success(
                "Saving primary analysis to .csv file: SUCCESS".ljust(77))

        except Exception as exception:
            logger.error(f"{exception}".ljust(77))

    logger.success(f"End analysis of '{data_name}'".ljust(77))
