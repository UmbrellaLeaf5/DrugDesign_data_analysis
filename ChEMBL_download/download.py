# from icecream import ic
from os import mkdir
from ChEMBL_download.functions import *  # from functions import *
from ChEMBL_download.analysis import *

# ic.disable()


def LoggerFormatUpdate() -> None:
    logger.remove()
    logger.add(sink=sys.stderr,
               format="[{time:DD.MM.YYYY HH:mm:ss}] <level>{level}</level>: <yellow>ChEMBL_download:</yellow> <white>{message}</white>")


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
            f"Downloading molecules with molecular weight in range [{less_limit}, {greater_limit})...")
        mw_mols_in_range: QuerySet = QuerySetMWRangeFilter(
            less_limit, greater_limit)
        logger.info(f"Amount: {len(mw_mols_in_range)}")  # type: ignore
        logger.success(
            f"Downloading molecules with molecular weight in range [{less_limit}, {greater_limit}): SUCCESS")

        try:
            logger.info("Collecting molecules to pandas.DataFrame()...")
            data = pd.DataFrame(mw_mols_in_range)  # type: ignore
            logger.success(
                "Collecting molecules to pandas.DataFrame(): SUCCESS")

            logger.info(
                "Collecting molecules from pandas.DataFrame to .csv file in results...")
            data = ExpandedFLDF(data)

            if (analysis_turn_on):
                DataAnalysisByColumns(data, f"Molecules with molecular weight in range [{
                                      less_limit}, {greater_limit})")
                LoggerFormatUpdate()

            file_name: str = f"results/range_{
                less_limit}_{greater_limit}_mw_mols.csv"

            data.to_csv(file_name, index=False)
            logger.success(
                "Collecting molecules from pandas.DataFrame to .csv file in results: SUCCESS")

        except Exception as exception:
            logger.error(exception)

    except Exception as exception:
        logger.error(exception)


def Download_ChEMBL(analysis_turn_on: bool = False):
    LoggerFormatUpdate()

    logger.info(f"{'-' * 25} ChEMBL downloading for DrugDesign {'-' * 25}")
    try:
        logger.info("Creating folder 'results'...")
        mkdir("results")
        logger.success("Creating folder 'results': SUCCESS")

    except Exception as exception:
        logger.warning(exception)

    logger.info(f"{'-' * 85}")

    mw_ranges: list[tuple[int, int]] = [
        (0, 100),
        # (100, 200), (200, 300),
        # (300, 400), (400, 500), (500, 600),
        # (600, 700), (700, 800), (800, 900),
        # (900, 1000), (1000, 12_546_42)
    ]

    for less_limit, greater_limit in mw_ranges:
        DownloadMWRange(less_limit, greater_limit, analysis_turn_on)
        logger.info(f"{'-' * 85}")

    logger.success(f"{'-' * 23} ChEMBL downloading for DrugDesign {'-' * 24}")


if __name__ == "__main__":
    Download_ChEMBL()
