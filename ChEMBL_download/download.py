# from icecream import ic
from os import mkdir
from ChEMBL_download.functions import *  # from functions import *
from ChEMBL_download.analysis import *   # from analysis import *

# ic.disable()


def LoggerFormatUpdate() -> None:
    logger.remove()
    logger.add(sink=sys.stdout,
               format="[{time:DD.MM.YYYY HH:mm:ss}]" +
                      " <yellow>ChEMBL_download:</yellow>" +
                      " <white>{message}</white>" +
                      " [<level>{level}</level>]")


def DownloadMWRange(less_limit: int = 0,
                    greater_limit: int = 12_546_42,
                    need_analysis: bool = False):
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
        mols_in_mw_range: QuerySet = QuerySetMWRangeFilter(
            less_limit, greater_limit)

        logger.info(("Amount:" +
                     f"{len(mols_in_mw_range)}").ljust(75))  # type: ignore
        logger.success(
            f"Downloading molecules with mw in range [{less_limit}, {greater_limit}): SUCCESS".ljust(75))

        try:
            logger.info(
                "Collecting molecules to pandas.DataFrame()...".ljust(75))
            data_frame = FreedFromDictionaryColumnsDF(pd.DataFrame(
                mols_in_mw_range))  # type: ignore
            logger.success(
                "Collecting molecules to pandas.DataFrame(): SUCCESS".ljust(75))

            logger.info(
                "Collecting molecules to .csv file in 'results'...".ljust(75))

            if (need_analysis):
                DataAnalysisByColumns(data_frame,
                                      f"mols_in_mw_range_{less_limit}_{greater_limit}")
                LoggerFormatUpdate()

            file_name: str = f"results/range_{
                less_limit}_{greater_limit}_mw_mols.csv"

            data_frame.to_csv(file_name, index=False)
            logger.success(
                "Collecting molecules to .csv file in 'results': SUCCESS".ljust(75))

        except Exception as exception:
            logger.error(f"{exception}".ljust(75))

    except Exception as exception:
        logger.error(f"{exception}".ljust(75))


def DownloadChEMBL(need_analysis: bool = False):
    """
    DownloadChEMBL - функция, которая скачивает необходимые для DrugDesign данные из базы ChEMBL

    Args:
        is_analysis_needed (bool, optional): необходимость вывода анализа в консоль. Defaults to False.
    """
    LoggerFormatUpdate()

    logger.info(f"{'-' * 20} ChEMBL downloading for DrugDesign {'-' * 20}")
    try:
        logger.info("Creating folder 'results'...".ljust(75))
        mkdir("results")
        logger.success("Creating folder 'results': SUCCESS".ljust(75))

    except Exception as exception:
        logger.warning(f"{exception}".ljust(75))

    if (need_analysis):
        try:
            logger.info("Creating folder 'analysis'...".ljust(75))
            mkdir("analysis")
            logger.success(
                "Creating folder 'analysis': SUCCESS".ljust(75))

        except Exception as exception:
            logger.warning(f"{exception}".ljust(75))

    logger.info(f"{'-' * 75}")

    mw_ranges: list[tuple[int, int]] = [
        (000, 190), (190, 215), (215, 230), (230, 240),
        (240, 250), (250, 260), (260, 267), (267, 273),
        (273, 280), (280, 285), (285, 290), (290, 295),
        (295, 299), (299, 303), (303, 307), (307, 311),
        (311, 315), (315, 319), (319, 323), (323, 327),
        (327, 330), (330, 334), (334, 337), (337, 340),
        (340, 343), (343, 346), (346, 349), (349, 352),
        (352, 355), (355, 359), (359, 363), (363, 367),
        (367, 371), (371, 375), (375, 379), (379, 383),
        (383, 387), (387, 391), (391, 395), (395, 399),
        (399, 403), (403, 407), (407, 411), (411, 415),
        (415, 419), (419, 423), (423, 427), (427, 431),
        (431, 435), (435, 439), (439, 443), (443, 447),
        (447, 451), (451, 456), (456, 461), (461, 466),
        (466, 471), (471, 476), (476, 481), (481, 487),
        (487, 493), (493, 499), (499, 506), (506, 514),
        (514, 522), (522, 531), (531, 541), (541, 552),
        (552, 565), (565, 579), (579, 596), (596, 617),
        (617, 648), (648, 693), (693, 758), (758, 868),
        (868, 1101), (868, 1200), (1200, 12_546_42)]

    for less_limit, greater_limit in mw_ranges:
        DownloadMWRange(less_limit, greater_limit, need_analysis)
        logger.info(f"{'-' * 75}")

    logger.success(f"{'-' * 20} ChEMBL downloading for DrugDesign {'-' * 20}")


if __name__ == "__main__":
    DownloadChEMBL()
