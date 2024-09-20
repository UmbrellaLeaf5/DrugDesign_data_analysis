import pandas as pd

from loguru import logger
import sys


def DataAnalysisByColumns(data: pd.DataFrame, data_name: str) -> None:
    logger.remove()
    logger.add(sink=sys.stderr,
               format="[{time:DD.MM.YYYY HH:mm:ss}] <level>{level}</level>: <cyan>ChEMBL_analysis:</cyan> <white>{message}</white>")

    logger.info(f"Start analysis of '{data_name}'...")
    for column in data.columns:
        logger.info("-" * 85)
        logger.info(f"Column                     : {column}")

        # тип данных
        try:
            data_type = data[column].dtype
            logger.info(f"Type of data               : {data_type}")

        except Exception as exception:
            logger.warning(f"Type of data:EXCEPTION     : {exception}")

        # количество ненулевых строк
        non_null_count = 0
        for value in data[column]:
            if value:
                non_null_count += 1

        logger.info(f"Non-empty strings          : {non_null_count}")

        # наиболее часто встречающееся значение
        try:
            mode_values = data[column].mode()
            if len(mode_values) > 0:
                common_value = mode_values[0]

            else:
                common_value = "nan"

            logger.info(f"Common value               : {common_value}")

        except Exception as exception:
            logger.warning(f"Common value:EXCEPTION     : {exception}")

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

            logger.info(f"Max value                  : {max_value}")
            logger.info(f"Min value                  : {min_value}")

        except Exception as exception:
            logger.warning(f"Max value:EXCEPTION        : {exception}")
            logger.warning(f"Min value:EXCEPTION        : {exception}")
