
# from icecream import ic

from ChEMBL_download_targets.functions import *

from Utils.decorators import IgnoreWarnings

# ic.disable()


@IgnoreWarnings
def DownloadChEMBLTargets(config: dict):
    """
    Скачивает информацию о мишенях из базы данных ChEMBL на основе конфигурации (`config.json`).

    Args:
        config (dict): словарь, содержащий параметры конфигурации для процесса скачивания.
    """

    targets_config = config["ChEMBL_download_targets"]
    activities_config = config["ChEMBL_download_activities"]

    UpdateLoggerFormat(targets_config["logger_label"],
                       targets_config["logger_color"])

    logger.info(f"{'-' * 21} ChEMBL downloading for DrugDesign {'-' * 21}")

    CreateFolder(targets_config["results_folder_name"])

    if config["need_primary_analysis"]:
        CreateFolder(
            f"{targets_config["results_folder_name"]}/{config["primary_analysis_folder_name"]}")

    if targets_config["download_activities"]:
        CreateFolder(activities_config["results_folder_name"])

    logger.info(f"{'-' * 77}")

    if config["testing_flag"]:
        targets_config["id_list"] = ["CHEMBL1951", "CHEMBL2034"]

    if not config["skip_downloaded"] or not IsFileInFolder(targets_config["results_folder_name"],
                                                           targets_config["results_file_name"]):
        if targets_config["download_all"]:
            # в случае пустого списка в DownloadTargetsFromIdList скачаются все
            targets_config["id_list"] = []

        DownloadTargetsFromIdList(config=config)

    else:
        logger.warning(
            f"{targets_config["results_file_name"]} is already downloaded, skip")

    logger.success(f"{'-' * 21} ChEMBL downloading for DrugDesign {'-' * 21}")
    logger.info(f"{'-' * 77}")
