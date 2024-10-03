# type: ignore

# from icecream import ic

from ChEMBL_download_targets.functions import *

from Utils.combine_funcs import *
from Utils.file_funcs import *


# ic.disable()

results_folder_name: str = "targets_results"
primary_analysis_folder_name: str = "primary_analysis"
combined_file_name: str = "combined_targets_data_from_ChEMBL"
logger_label: str = "ChEMBL__targets"


def DownloadChEMBLTargets(need_primary_analysis: bool = False,
                          skip_downloaded_files: bool = False,
                          download_all: bool = False,
                          testing_flag: bool = False):

    LoggerFormatUpdate(logger_label, "yellow")

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

    id_list: list[str] = ["CHEMBL220", "CHEMBL251", "CHEMBL229", "CHEMBL1867",
                          "CHEMBL213", "CHEMBL210", "CHEMBL1871", "CHEMBL216",
                          "CHEMBL211", "CHEMBL245", "CHEMBL218", "CHEMBL253",
                          "CHEMBL2056", "CHEMBL217", "CHEMBL252", "CHEMBL231",
                          "CHEMBL214", "CHEMBL1898", "CHEMBL224", "CHEMBL1833",
                          "CHEMBL240", "CHEMBL258", "CHEMBL1951", "CHEMBL4777",
                          "CHEMBL2034", "CHEMBL236", "CHEMBL233", "CHEMBL222", "CHEMBL228"]

    if testing_flag:
        id_list = ["CHEMBL220", "CHEMBL251"]

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
