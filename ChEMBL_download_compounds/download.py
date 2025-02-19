# from icecream import ic

from ChEMBL_download_compounds.functions import *

from Utils.decorators import IgnoreWarnings

# ic.disable()


@IgnoreWarnings
def DownloadChEMBLCompounds(config: dict):
    """
    Скачивает информацию о соединениях из базы данных ChEMBL на основе конфигурации (`config.json`).

    Args:
        config (dict): словарь, содержащий параметры конфигурации для процесса скачивания.
    """

    compounds_config = config["ChEMBL_download_compounds"]

    if compounds_config["delete_after_combining"] and not compounds_config["need_combining"]:
        raise ValueError(
            "DownloadChEMBLCompounds: delete_after_combining=True but need_combine=False")

    if config["skip_downloaded"] and config["need_primary_analysis"]:
        raise ValueError(
            "DownloadChEMBLCompounds: skip_downloaded_files=True, nothing to analyse")

    UpdateLoggerFormat(compounds_config["logger_label"], compounds_config["logger_color"])

    logger.info(f"{'-' * 21} ChEMBL downloading for DrugDesign {'-' * 21}")

    CreateFolder(compounds_config["results_folder_name"])

    if config["need_primary_analysis"]:
        CreateFolder(
            f"{compounds_config["results_folder_name"]}/{config["primary_analysis_folder_name"]}")

    logger.info(f"{'-' * 77}")

    if config["testing_flag"]:
        compounds_config["mw_ranges"] = [[0, 50], [50, 75]]

    for mw_range in compounds_config["mw_ranges"]:
        less_limit = mw_range[0]
        greater_limit = mw_range[1]

        if not config["skip_downloaded"] or not IsFileInFolder(f"{compounds_config["results_folder_name"]}",
                                                               f"range_{less_limit}_{greater_limit}_mw_mols.csv"):
            DownloadCompoundsByMWRange(
                less_limit,
                greater_limit,
                need_primary_analysis=config["need_primary_analysis"],
                print_to_console=(config["print_to_console_verbosely"] or config["testing_flag"]),
                results_folder_name=compounds_config["results_folder_name"],
                primary_analysis_folder_name=config["primary_analysis_folder_name"],
                logger_label=compounds_config["logger_label"],
                logger_color=compounds_config["logger_color"])

        else:
            logger.warning(f"Molecules with mw in range [{less_limit}, "
                           f"{greater_limit}) is already downloaded, skip")

        logger.info(f"{'-' * 77}")

    if compounds_config["need_combining"]:
        CombineCSVInFolder(compounds_config["results_folder_name"],
                           compounds_config["combined_file_name"],
                           skip_downloaded_files=config["skip_downloaded"],
                           print_to_console=(config["print_to_console_verbosely"] or config["testing_flag"]))

        UpdateLoggerFormat(compounds_config["logger_label"], compounds_config["logger_color"])

    if compounds_config["delete_after_combining"]:
        logger.info(
            f"Deleting files after combining in '{compounds_config["results_folder_name"]}'...")

        try:
            DeleteFilesInFolder(compounds_config["results_folder_name"], [
                                f"{compounds_config["combined_file_name"]}.csv"])
            logger.success(
                f"Deleting files after combining in '{compounds_config["results_folder_name"]}'")

        except Exception as exception:
            LogException(exception)

    logger.success(f"{'-' * 21} ChEMBL downloading for DrugDesign {'-' * 21}")
    logger.info(f"{'-' * 77}")
