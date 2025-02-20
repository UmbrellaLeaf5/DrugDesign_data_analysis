from ChEMBL_download_cell_lines.functions import *

from Utils.decorators import IgnoreWarnings
from Utils.files_funcs import IsFileInFolder

from Configurations.config import Config


@IgnoreWarnings
def DownloadChEMBLCellLines(config: Config):
    """
    Скачивает информацию о клеточных линиях из базы данных ChEMBL 
    на основе конфигурации (`config.json`).

    Args:
        config (Config): словарь, содержащий параметры конфигурации для процесса скачивания.
    """

    cell_lines_config: Config = config["ChEMBL_download_cell_lines"]
    activities_config: Config = config["ChEMBL_download_activities"]

    UpdateLoggerFormat(cell_lines_config["logger_label"],
                       cell_lines_config["logger_color"])

    logger.info(f"{'-' * 21} ChEMBL downloading for DrugDesign {'-' * 21}")

    os.makedirs(cell_lines_config["results_folder_name"], exist_ok=True)

    if cell_lines_config["download_activities"]:
        os.makedirs(activities_config["results_folder_name"], exist_ok=True)

    if config["testing_flag"]:
        cell_lines_config["id_list"] = ["CHEMBL4295386", "CHEMBL3307781"]

    if not config["skip_downloaded"] or\
        not IsFileInFolder(f"{cell_lines_config["results_file_name"]}.csv",
                           f"{cell_lines_config["results_folder_name"]}"):
        if cell_lines_config["download_all"]:
            # в случае пустого списка в DownloadCellLinesFromIdList скачаются все
            cell_lines_config["id_list"] = []

        DownloadCellLinesFromIdList(config)

    else:
        if config["verbose_print"]:
            logger.info(
                f"{cell_lines_config["results_file_name"]} is already downloaded, skip.")

    logger.success(f"{'-' * 21} ChEMBL downloading for DrugDesign {'-' * 21}")
    logger.info(f"{'-' * 77}")
