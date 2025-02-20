from ChEMBL_download_targets.functions import *

from Utils.decorators import IgnoreWarnings
from Utils.files_funcs import IsFileInFolder, os


@IgnoreWarnings
def DownloadChEMBLTargets(config: Config):
    """
    Скачивает информацию о мишенях из базы данных ChEMBL 
    на основе конфигурации (`config.json`).

    Args:
        config (Config): словарь, содержащий параметры конфигурации для процесса скачивания.
    """

    targets_config: Config = config["ChEMBL_download_targets"]
    activities_config: Config = config["ChEMBL_download_activities"]

    UpdateLoggerFormat(targets_config["logger_label"],
                       targets_config["logger_color"])

    logger.info(f"{'-' * 21} ChEMBL downloading for DrugDesign {'-' * 21}")

    os.makedirs(targets_config["results_folder_name"], exist_ok=True)

    if targets_config["download_activities"]:
        os.makedirs(activities_config["results_folder_name"], exist_ok=True)

    if config["testing_flag"]:
        targets_config["id_list"] = ["CHEMBL1951", "CHEMBL2034"]

    if not config["skip_downloaded"] or not IsFileInFolder(
            targets_config["results_file_name"],
            targets_config["results_folder_name"]):
        if targets_config["download_all"]:
            # в случае пустого списка в DownloadTargetsFromIdList скачаются все
            targets_config["id_list"] = []

        DownloadTargetsFromIdList(config)

    else:
        if config["verbose_print"]:
            logger.info(
                f"{targets_config["results_file_name"]} is already downloaded, skip")

    logger.success(f"{'-' * 21} ChEMBL downloading for DrugDesign {'-' * 21}")
    logger.info(f"{'-' * 77}")
