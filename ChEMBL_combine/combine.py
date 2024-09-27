import os
import pandas as pd

from loguru import logger
import sys


def CombineChEMBL() -> None:
    """
    CombineChEMBL - функция, которая объединяет все .csv файлы в один
    """

    logger.remove()

    logger.add(sink=sys.stderr,
               format="[{time:DD.MM.YYYY HH:mm:ss}]" +
               " <blue>ChEMBL__combine:</blue>" +
               " <white>{message}</white>" +
               " [<level>{level}</level>]")

    logger.info(
        f"{'-' * 22} ChEMBL combining for DrugDesign {'-' * 22}")

    folder_path: str = f"results/"

    combined_df = pd.DataFrame()

    logger.info(f"{'-' * 77}")

    for file_name in os.listdir(folder_path):
        if file_name.endswith('.csv') and file_name != f"combined_data_from_ChEMBL.csv":

            logger.info(f"Opening '{file_name}'...".ljust(77))
            file_path: str = os.path.join(folder_path, file_name)
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
        "Collecting molecules to .csv file in 'results'...".ljust(77))
    try:
        combined_df.to_csv(
            f"{folder_path}combined_data_from_ChEMBL.csv", index=False)
        logger.success(
            f"Collecting molecules to .csv file in 'results': SUCCESS".ljust(77))

    except Exception as exception:
        logger.error(f"{exception}".ljust(77))

    logger.info(
        f"{'-' * 22} ChEMBL combining for DrugDesign {'-' * 22}")


if __name__ == "__main__":
    CombineChEMBL()
