# type: ignore

import os
import pandas as pd

try:
    from logger_funcs import *

except ImportError:
    from Utils.logger_funcs import *


def CombineCSVInFolder(folder_name: str, combined_file_name: str,
                       logger_label: str = "ChEMBL__combine") -> None:
    """
    Склеивает все .csv файлы в папке в один

    Args:
        folder_name (str): имя папки с .csv файлами
        combined_file_name (str): имя склеенного .csv файла
        logger_label (str, optional): текст заголовка для логирования. Defaults to "ChEMBL__combine".
    """

    LoggerFormatUpdate(logger_label, "blue")

    logger.info(f"Start combining all downloads...".ljust(77))
    logger.info(f"{' ' * 77}")

    combined_df = pd.DataFrame()

    for file_name in os.listdir(folder_name):
        if file_name.endswith('.csv') and file_name != f"{combined_file_name}.csv":

            logger.info(f"Opening '{file_name}'...".ljust(77))
            file_path: str = os.path.join(folder_name, file_name)
            logger.success(f"Opening '{file_name}': SUCCESS".ljust(77))

            logger.info(
                f"Collecting '{file_name}' to pandas.DataFrame()...".ljust(77))
            try:
                df = pd.read_csv(file_path, low_memory=False)
                logger.success(
                    f"Collecting '{file_name}' to pandas.DataFrame(): SUCCESS".ljust(77))

            except Exception as exception:
                logger.error(f"{exception}".ljust(77))

            logger.info(
                f"Concatenating '{file_name}' to combined_data_frame...".ljust(77))
            try:
                combined_df = pd.concat([combined_df, df], ignore_index=True)
                logger.success(
                    f"Concatenating '{file_name}' to combined_data_frame: SUCCESS".ljust(77))

            except Exception as exception:
                logger.error(f"{exception}".ljust(77))

            logger.info(f"{'-' * 77}")

    logger.info(
        f"Collecting to combined .csv file in '{folder_name}'...".ljust(77))
    try:
        combined_df.to_csv(
            f"{folder_name}/{combined_file_name}.csv", index=False)
        logger.success(
            f"Collecting to combined .csv file in '{folder_name}': SUCCESS".ljust(77))

    except Exception as exception:
        logger.error(f"{exception}".ljust(77))

    logger.info(f"{' ' * 77}")
    logger.success(f"End combining all downloads".ljust(77))
