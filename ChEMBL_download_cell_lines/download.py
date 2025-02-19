# from icecream import ic

from ChEMBL_download_cell_lines.functions import *

from Utils.decorators import IgnoreWarnings

# ic.disable()


@IgnoreWarnings
def DownloadChEMBLCellLines(config: dict):
    """
    Скачивает информацию о клеточных линиях из базы данных ChEMBL на основе конфигурации (`config.json`).

    Args:
        config (dict): словарь, содержащий параметры конфигурации для процесса скачивания.
    """

    cell_lines_config: dict = config["ChEMBL_download_cell_lines"]
    activities_config: dict = config["ChEMBL_download_activities"]

    UpdateLoggerFormat(cell_lines_config["logger_label"],
                       cell_lines_config["logger_color"])

    logger.info(f"{'-' * 21} ChEMBL downloading for DrugDesign {'-' * 21}")

    CreateFolder(cell_lines_config["results_folder_name"])

    if config["need_primary_analysis"]:
        CreateFolder(
            f"{cell_lines_config["results_folder_name"]}/{config["primary_analysis_folder_name"]}")

    if cell_lines_config["download_activities"]:
        CreateFolder(activities_config["results_folder_name"])

    logger.info(f"{'-' * 77}")

    if config["testing_flag"]:
        cell_lines_config["id_list"] = ["CHEMBL4295386", "CHEMBL3307781"]

    if not config["skip_downloaded"] or not IsFileInFolder(f"{cell_lines_config["results_file_name"]}.csv",
                                                           f"{cell_lines_config["results_folder_name"]}"):
        if cell_lines_config["download_all"]:
            # в случае пустого списка в DownloadCellLinesFromIdList скачаются все
            cell_lines_config["id_list"] = []

        DownloadCellLinesFromIdList(config=config)

    else:
        logger.warning(
            f"{cell_lines_config["results_file_name"]} is already downloaded, skip")

    logger.success(f"{'-' * 21} ChEMBL downloading for DrugDesign {'-' * 21}")
    logger.info(f"{'-' * 77}")
