# type: ignore

# from icecream import ic

from ChEMBL_download_compounds.functions import *

from Utils.decorators import IgnoreWarnings

# ic.disable()

results_folder_name: str = "results/compounds"
primary_analysis_folder_name: str = "primary_analysis"
combined_file_name: str = "combined_compounds_data_from_ChEMBL"
logger_label: str = "ChEMBL_compound"


@IgnoreWarnings
def DownloadChEMBLCompounds(need_primary_analysis: bool = False,
                            need_combining: bool = True,
                            delete_downloaded_after_combining: bool = True,
                            skip_downloaded_files: bool = False,
                            testing_flag: bool = False):
    """
    Скачивает необходимые молекулы из базы ChEMBL

    Args:
        need_primary_analysis (bool, optional): нужен ли первичный анализ скачанных файлов. Defaults to False.
        need_combining (bool, optional): нужно ли собирать все скачанные файлы в один. Defaults to True.
        delete_downloaded_after_combining (bool, optional): нужно ли удалять все скачанные файлы после комбинирования. Defaults to True.
        skip_downloaded_files (bool, optional): пропускать ли уже скачанные файлы. Defaults to False.
        testing_flag (bool, optional): спец. флаг для тестирования функционала. Defaults to False.
    """

    if delete_downloaded_after_combining and not need_combining:
        raise ValueError(
            "DownloadChEMBLCompounds: delete_downloaded_after_combining=True but need_combine=False")

    if skip_downloaded_files and need_primary_analysis:
        raise ValueError(
            "DownloadChEMBLCompounds: skip_downloaded_files=True, nothing to analyse")

    UpdateLoggerFormat(logger_label, "yellow")

    logger.info(f"{'-' * 21} ChEMBL downloading for DrugDesign {'-' * 21}")

    CreateFolder(results_folder_name)

    if need_primary_analysis:
        CreateFolder(f"{results_folder_name}/{primary_analysis_folder_name}")

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
            DownloadCompoundsByMWRange(
                less_limit, greater_limit, need_primary_analysis=need_primary_analysis)

        else:
            logger.warning(f"Molecules with mw in range [{less_limit}, {
                           greater_limit}) is already downloaded, skip".ljust(77))

        logger.info(f"{'-' * 77}")

    if need_combining:
        CombineCSVInFolder(results_folder_name,
                           combined_file_name,
                           skip_downloaded_files=skip_downloaded_files)

        UpdateLoggerFormat(logger_label, "yellow")

    if delete_downloaded_after_combining:
        logger.info(f"Deleting files after combining in '{
                    results_folder_name}'...".ljust(77))

        try:
            DeleteFilesInFolder(results_folder_name,
                                f"{combined_file_name}.csv")
            logger.success(
                f"Deleting files after combining in '{results_folder_name}'".ljust(77))

        except Exception as exception:
            logger.error(f"{exception}".ljust(77))

    logger.success(f"{'-' * 21} ChEMBL downloading for DrugDesign {'-' * 21}")
