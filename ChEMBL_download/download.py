# from icecream import ic
from os import mkdir
from ChEMBL_download.functions import *  # from functions import *
from ChEMBL_download.analysis import *

# ic.disable()


def LoggerFormatUpdate() -> None:
    logger.remove()
    logger.add(sink=sys.stderr,
               format="[{time:DD.MM.YYYY HH:mm:ss}] <yellow>ChEMBL_download:</yellow> <white>{message}</white> [<level>{level}</level>]")


def DownloadMWRange(less_limit: int = 0, greater_limit: int = 12_546_42, analysis_turn_on: bool = False):
    """
    DownloadMolecularWeightRange - функция, которая скачивает молекулы в .csv файл с выводом
    информации из базы ChEMBL по диапазону ( [): полуинтервалу) молекулярного веса

    Args:
        less_limit (int, optional): нижняя граница. Defaults to 0
        greater_limit (int, optional): верхняя граница. Defaults to 12_546_42
    """

    try:
        logger.info(
            f"Downloading molecules with mw in range [{less_limit}, {greater_limit})...".ljust(75))
        mw_mols_in_range: QuerySet = QuerySetMWRangeFilter(
            less_limit, greater_limit)

        logger.info(("Amount:" +
                    f"{len(mw_mols_in_range)}").ljust(75))  # type: ignore
        logger.success(
            f"Downloading molecules with mw in range [{less_limit}, {greater_limit}): SUCCESS".ljust(75))

        try:
            logger.info(
                "Collecting molecules to pandas.DataFrame()...".ljust(75))
            data = pd.DataFrame(mw_mols_in_range)  # type: ignore
            logger.success(
                "Collecting molecules to pandas.DataFrame(): SUCCESS".ljust(75))

            logger.info(
                "Collecting molecules from pandas.DataFrame to .csv file in results...".ljust(75))
            data = ExpandedFLDF(data)

            if (analysis_turn_on):
                DataAnalysisByColumns(data, f"mols_with_mw_in_range_{less_limit}_{greater_limit}",
                                      print_to_console=False, save_to_csv=True)
                LoggerFormatUpdate()

            file_name: str = f"results/range_{
                less_limit}_{greater_limit}_mw_mols.csv"

            data.to_csv(file_name, index=False)
            logger.success(
                "Collecting molecules to .csv file in results: SUCCESS".ljust(75))

        except Exception as exception:
            logger.error(f"{exception}".ljust(75))

    except Exception as exception:
        logger.error(f"{exception}".ljust(75))


def Download_ChEMBL(analysis_turn_on: bool = False):
    """
    Download_ChEMBL - функция, которая скачивает необходимые для DrugDesign данные из базы ChEMBL

    Args:
        analysis_turn_on (bool, optional): необходимость вывода анализа в консоль. Defaults to False.
    """
    LoggerFormatUpdate()

    logger.info(f"{'-' * 20} ChEMBL downloading for DrugDesign {'-' * 20}")
    try:
        logger.info("Creating folder 'results'...".ljust(75))
        mkdir("results")
        logger.success("Creating folder 'results': SUCCESS".ljust(75))

    except Exception as exception:
        logger.warning(f"{exception}".ljust(75))

    logger.info(f"{'-' * 75}")

    mw_ranges: list[tuple[int, int]] = [
        (0, 100),
        # (100, 200), (200, 300),
        # (300, 400), (400, 500), (500, 600),
        # (600, 700), (700, 800), (800, 900),
        # (900, 1000), (1000, 12_546_42)
    ]

    for less_limit, greater_limit in mw_ranges:
        DownloadMWRange(less_limit, greater_limit, analysis_turn_on)
        logger.info(f"{'-' * 75}")

    logger.success(f"{'-' * 20} ChEMBL downloading for DrugDesign {'-' * 20}")


if __name__ == "__main__":
    Download_ChEMBL()
