# type: ignore

# from icecream import ic

try:
    from functions import *
    from primary_analysis import *
    from combine import *

except ImportError:
    from ChEMBL_download_compounds.functions import *
    from ChEMBL_download_compounds.primary_analysis import *
    from ChEMBL_download_compounds.combine import *


# ic.disable()

results_folder_name: str = "compounds_results"
primary_analysis_folder_name: str = "primary_analysis"


def LoggerFormatUpdate() -> None:
    logger.remove()
    logger.add(sink=sys.stdout,
               format="[{time:DD.MM.YYYY HH:mm:ss}]" +
                      " <yellow>ChEMBL_download:</yellow>" +
                      " <white>{message}</white>" +
                      " [<level>{level}</level>]")


def DownloadMWRangeCompounds(less_limit: int = 0,
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
            f"Downloading molecules with mw in range [{less_limit}, {greater_limit})...".ljust(77))
        mols_in_mw_range: QuerySet = QuerySetMWRangeFilterCompounds(
            less_limit, greater_limit)

        logger.info(
            ("Amount:" + f"{len(mols_in_mw_range)}").ljust(77))
        logger.success(
            f"Downloading molecules with mw in range [{less_limit}, {greater_limit}): SUCCESS".ljust(77))

        try:
            logger.info(
                "Collecting molecules to pandas.DataFrame()...".ljust(77))
            data_frame = ExpandedFromDictionaryColumnsDFCompounds(pd.DataFrame(
                mols_in_mw_range))
            logger.success(
                "Collecting molecules to pandas.DataFrame(): SUCCESS".ljust(77))

            logger.info(
                f"Collecting molecules to .csv file in '{results_folder_name}'...".ljust(77))

            if need_analysis:
                DataAnalysisByColumns(data_frame,
                                      f"mols_in_mw_range_{less_limit}_{greater_limit}")
                LoggerFormatUpdate()

            file_name: str = f"{results_folder_name}/range_{
                less_limit}_{greater_limit}_mw_mols.csv"

            data_frame.to_csv(file_name, index=False)
            logger.success(
                f"Collecting molecules to .csv file in '{results_folder_name}': SUCCESS".ljust(77))

        except Exception as exception:
            logger.error(f"{exception}".ljust(77))

    except Exception as exception:
        logger.error(f"{exception}".ljust(77))


def DownloadChEMBLCompounds(need_primary_analysis: bool = False,
                            need_combining: bool = True,
                            delete_downloaded_after_combining: bool = True,
                            skip_downloaded_files: bool = False,
                            testing_flag: bool = False):
    """
    DownloadChEMBL - функция, которая скачивает необходимые для DrugDesign данные из базы ChEMBL

    Args:
        need_primary_analysis (bool, optional): нужен ли первичный анализ скачанных файлов. Defaults to False.
        need_combining (bool, optional): нужно ли собирать все скачанные файлы в один. Defaults to True.
        delete_downloaded_after_combining (bool, optional): нужно ли удалять все скачанные файлы после комбинирования. Defaults to True.
        skip_downloaded_files (bool, optional): нужно ли пропустить уже скачанные файлы в папке compounds_results. Defaults to False.
        testing_flag (bool, optional): [скачивание только двух таблиц для тестирования функционала]. Defaults to False.
    """

    if delete_downloaded_after_combining and not need_combining:
        raise ValueError(
            "DownloadChEMBL: delete_downloaded_after_combining=True but need_combine=False")

    LoggerFormatUpdate()

    logger.info(f"{'-' * 21} ChEMBL downloading for DrugDesign {'-' * 21}")
    try:
        logger.info(f"Creating folder '{results_folder_name}'...".ljust(77))
        os.mkdir(results_folder_name)
        logger.success(f"Creating folder '{
                       results_folder_name}': SUCCESS".ljust(77))

    except Exception as exception:
        logger.warning(f"{exception}".ljust(77))

    if need_primary_analysis:
        try:
            logger.info(f"Creating folder '{
                        primary_analysis_folder_name}'...".ljust(77))
            os.mkdir(f"{results_folder_name}/{primary_analysis_folder_name}")
            logger.success(
                f"Creating folder '{primary_analysis_folder_name}': SUCCESS".ljust(77))

        except Exception as exception:
            logger.warning(f"{exception}".ljust(77))

    logger.info(f"{'-' * 77}")

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
        (868, 1101), (1101, 1200), (1200, 12_546_42)]

    if testing_flag:
        mw_ranges = [(0, 50), (50, 75)]

    for less_limit, greater_limit in mw_ranges:
        if not skip_downloaded_files or not IsFileInFolder(f"{results_folder_name}",
                                                           f"range_{less_limit}_{greater_limit}_mw_mols.csv"):
            DownloadMWRangeCompounds(
                less_limit, greater_limit, need_primary_analysis)
        else:
            logger.warning(f"Molecules with mw in range [{less_limit}, {
                           greater_limit}) is already downloaded, skip".ljust(77))

        logger.info(f"{'-' * 77}")

    if need_combining:
        CombineChEMBLCompounds()
        LoggerFormatUpdate()

    if delete_downloaded_after_combining:
        logger.info(f"Deleting files after combining in '{
                    results_folder_name}'...".ljust(77))

        try:
            DeleteFilesInFolder(results_folder_name,
                                "combined_compounds_data_from_ChEMBL.csv")
            logger.success(
                f"Deleting files after combining in '{results_folder_name}'".ljust(77))

        except Exception as exception:
            logger.error(f"{exception}".ljust(77))

    logger.success(f"{'-' * 21} ChEMBL downloading for DrugDesign {'-' * 21}")


if __name__ == "__main__":
    DownloadChEMBLCompounds(need_primary_analysis=True)
