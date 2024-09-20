from numpy import save
import pandas as pd

from loguru import logger
import sys

from os import mkdir


def DataAnalysisByColumns(data: pd.DataFrame, data_name: str,
                          print_to_console: bool = True, save_to_csv: bool = False) -> None:
    """
    DataAnalysisByColumns - функция, которая выводит в консоль краткую сводку о столбцах pandas.DataFrame()
    в консоль и сохраняет ее в .csv файл по следующим признакам: тип данных, количество ненулевых строк,
    наиболее часто встречающееся значение, максимальное и минимальное значения

    Args:
        data (pd.DataFrame): изначальная табличка
        data_name (str): имя таблицы (нужно для вывода и названия файла)
        print_to_console (bool, optional): необходимость вывода сводки в консоль. Defaults to True.
        save_to_csv (bool, optional): необходимость сохранения сводки в файл. Defaults to False.
    """

    logger.remove()
    logger.add(sink=sys.stderr,
               format="[{time:DD.MM.YYYY HH:mm:ss}] <cyan>ChEMBL_analysis:</cyan> <white>{message}</white> [<level>{level}</level>]")

    logger.info(f"Start analysis of '{data_name}'...".ljust(75))

    summary: dict = {'Column': [],
                     'Data type': [],
                     'Non-empty strings': [],
                     'Common value': [],
                     'Max value': [],
                     'Min value': []}

    for column in data.columns:
        # имя столбца
        if print_to_console:
            logger.info("-" * 85)
            logger.info(f"{"Column".ljust(30)}: {column}".ljust(75))

        if save_to_csv:
            summary['Column'].append(column)

        # тип данных
        try:
            data_type = data[column].dtype

            if print_to_console:
                logger.info(f"{"Type of data".ljust(30)}: {
                            data_type}".ljust(75))

            if save_to_csv:
                summary['Data type'].append(data_type)

        except Exception as exception:
            if print_to_console:
                logger.warning(
                    f"{"Type of data:EXCEPTION".ljust(30)}: {exception}".ljust(75))

            if save_to_csv:
                summary['Data type'].append("EXCEPTION")

        # количество ненулевых строк
        non_null_count = 0
        for value in data[column]:
            if value:
                non_null_count += 1

        if print_to_console:
            logger.info(f"{"Non-empty strings".ljust(30)
                           }: {non_null_count}".ljust(75))

        if save_to_csv:
            summary['Non-empty strings'].append(non_null_count)

        # наиболее часто встречающееся значение
        try:
            mode_values = data[column].mode()
            if len(mode_values) > 0:
                common_value = mode_values[0]

            else:
                common_value = ""

            if print_to_console:
                logger.info(f"{"Common value".ljust(30)}: {
                            common_value}".ljust(75))

            if save_to_csv:
                summary['Common value'].append(common_value)

        except Exception as exception:
            if print_to_console:
                logger.warning(
                    f"{"Common value:EXCEPTION".ljust(30)}: {exception}".ljust(75))

            if save_to_csv:
                summary['Common value'].append("")

        # максимальное и минимальное значения
        try:
            try:
                max_value = data[column].max()
                min_value = data[column].min()

            except TypeError:
                max_value = None
                min_value = None

                for value in data[column]:
                    if value is None:
                        continue

                    elif isinstance(value, (list, str)):
                        if max_value is None or len(value) > len(max_value):
                            max_value = value
                        if min_value is None or len(value) < len(min_value):
                            min_value = value

            if print_to_console:
                logger.info(f"{"Max value".ljust(30)}: {max_value}".ljust(75))
                logger.info(f"{"Min value".ljust(30)}: {min_value}".ljust(75))

            if save_to_csv:
                summary['Max value'].append(max_value)
                summary['Min value'].append(min_value)

        except Exception as exception:
            if print_to_console:
                logger.warning(
                    f"{"Max value:EXCEPTION".ljust(30)}: {exception}".ljust(75))
                logger.warning(
                    f"{"Min value:EXCEPTION".ljust(30)}: {exception}".ljust(75))

            if save_to_csv:
                summary['Max value'].append("")
                summary['Min value'].append("")

    if save_to_csv:
        try:
            logger.info("Creating folder 'analysis'...".ljust(75))
            mkdir("analysis")
            logger.success("Creating folder 'analysis': SUCCESS".ljust(75))

        except Exception as exception:
            logger.warning(f"{exception}".ljust(75))

        try:
            logger.info("Saving summary analysis to .csv file...".ljust(75))

            file_name: str = f"analysis/{data_name}_analysis.csv"

            pd.DataFrame(summary).to_csv(file_name, index=False)

            logger.success(
                "Saving summary analysis to .csv file: SUCCESS".ljust(75))

        except Exception as exception:
            logger.error(f"{exception}".ljust(75))

    logger.success(f"End analysis of '{data_name}'".ljust(75))
