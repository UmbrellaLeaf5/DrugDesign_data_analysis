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
               format="[{time:DD.MM.YYYY HH:mm:ss}] <level>{level}</level>: <cyan>ChEMBL_analysis:</cyan> <white>{message}</white>")

    logger.info(f"Start analysis of '{data_name}'...")

    summary: dict = {'Column': [],
                     'Data type': [],
                     'Non-empty strings': [],
                     'Common value': [],
                     'Max value': [],
                     'Min value': []}

    for column in data.columns:
        if print_to_console:
            logger.info("-" * 85)
            logger.info(f"Column                     : {column}")

        if save_to_csv:
            summary['Column'].append(column)

        # тип данных
        try:
            data_type = data[column].dtype

            if print_to_console:
                logger.info(f"Type of data               : {data_type}")

            if save_to_csv:
                summary['Data type'].append(data_type)

        except Exception as exception:
            if print_to_console:
                logger.warning(f"Type of data:EXCEPTION     : {exception}")

            if save_to_csv:
                summary['Data type'].append("EXCEPTION")

        # количество ненулевых строк
        non_null_count = 0
        for value in data[column]:
            if value:
                non_null_count += 1

        if print_to_console:
            logger.info(f"Non-empty strings          : {non_null_count}")

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
                logger.info(f"Common value               : {common_value}")

            if save_to_csv:
                summary['Common value'].append(common_value)

        except Exception as exception:
            if print_to_console:
                logger.warning(f"Common value:EXCEPTION     : {exception}")

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
                logger.info(f"Max value                  : {max_value}")
                logger.info(f"Min value                  : {min_value}")

            if save_to_csv:
                summary['Max value'].append(max_value)
                summary['Min value'].append(min_value)

        except Exception as exception:
            if print_to_console:
                logger.warning(f"Max value:EXCEPTION        : {exception}")
                logger.warning(f"Min value:EXCEPTION        : {exception}")

            if save_to_csv:
                summary['Max value'].append("EXCEPTION")
                summary['Min value'].append("EXCEPTION")

    if save_to_csv:
        try:
            logger.info("Creating folder 'analysis'...")
            mkdir("analysis")
            logger.success("Creating folder 'analysis': SUCCESS")

        except Exception as exception:
            if print_to_console:
                logger.warning(exception)

        try:
            logger.info("Saving summary analysis to .csv file...")

            file_name: str = f"analysis/{data_name}_analysis.csv"

            pd.DataFrame(summary).to_csv(file_name, index=False)

            logger.success("Saving summary analysis to .csv file: SUCCESS")

        except Exception as exception:
            logger.error(exception)

    logger.success(f"End analysis of '{data_name}'")
