# type: ignore

# from icecream import ic

from ChEMBL_download_targets.functions import *

# ic.disable()

results_folder_name: str = "targets_results"
primary_analysis_folder_name: str = "primary_analysis"
combined_file_name: str = "combined_targets_data_from_ChEMBL"
logger_label: str = "ChEMBL__targets"


def DownloadChEMBLTargets(need_primary_analysis: bool = False,
                          download_all: bool = False,
                          download_activities: bool = True,
                          skip_downloaded_files: bool = False,
                          testing_flag: bool = False) -> None:
    """
    Скачивает необходимые цели из базы ChEMBL

    Args:
        need_primary_analysis (bool, optional): нужен ли первичный анализ скачанных файлов. Defaults to False.
        download_all (bool, optional): скачивать ли все цели (или использовать только те, что из списка). Defaults to False.
        download_activities (bool, optional): скачивать ли наборы активностей к целям (по IC50 и Ki). Defaults to True.
        skip_downloaded_files (bool, optional): пропускать ли уже скачанные файлы. Defaults to False.
        testing_flag (bool, optional): спец. флаг для тестирования функционала. Defaults to False.
    """

    UpdateLoggerFormat(logger_label, "yellow")

    logger.info(f"{'-' * 21} ChEMBL downloading for DrugDesign {'-' * 21}")

    CreateFolder(results_folder_name)

    if need_primary_analysis:
        CreateFolder(f"{results_folder_name}/{primary_analysis_folder_name}",
                     primary_analysis_folder_name)

    if download_activities:
        CreateFolder(f"{results_folder_name}/activities",
                     "activities")

    logger.info(f"{'-' * 77}")

    id_list: list[str] = ["CHEMBL220", "CHEMBL251", "CHEMBL229", "CHEMBL1867",
                          "CHEMBL213", "CHEMBL210", "CHEMBL1871", "CHEMBL216",
                          "CHEMBL211", "CHEMBL245", "CHEMBL218", "CHEMBL253",
                          "CHEMBL2056", "CHEMBL217", "CHEMBL252", "CHEMBL231",
                          "CHEMBL214", "CHEMBL1898", "CHEMBL224", "CHEMBL1833",
                          "CHEMBL240", "CHEMBL258", "CHEMBL1951", "CHEMBL4777",
                          "CHEMBL2034", "CHEMBL236", "CHEMBL233", "CHEMBL222", "CHEMBL228"]

    if testing_flag:
        id_list = ["CHEMBL1951", "CHEMBL2034"]

    if not skip_downloaded_files or not IsFileInFolder(f"{results_folder_name}",
                                                       "targets_data_from_ChEMBL.csv"):
        if download_all:
            DownloadAllTargets()

        else:
            DownloadTargetsFromIdList(
                id_list, need_primary_analysis=need_primary_analysis)

    else:
        logger.warning(
            f"targets_data_from_ChEMBL is already downloaded, skip".ljust(77))

    logger.success(f"{'-' * 21} ChEMBL downloading for DrugDesign {'-' * 21}")
