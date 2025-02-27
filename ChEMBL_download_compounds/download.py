from ChEMBL_download_compounds.functions import *

from Utils.decorators import IgnoreWarnings
from Utils.files_funcs import CombineCSVInFolder, DeleteFilesInFolder, \
    IsFileInFolder, os
from Utils.verbose_logger import v_logger, LogMode

from Configurations.config import config, Config


@IgnoreWarnings
@ReTry(attempts_amount=1)
def DownloadChEMBLCompounds():
    """
    Скачивает информацию о соединениях из базы данных ChEMBL
    на основе конфигурации (`config.json`).
    """

    compounds_config: Config = config["ChEMBL_download_compounds"]

    if compounds_config["delete_after_combining"] and\
            not compounds_config["need_combining"]:
        raise ValueError(
            "DownloadChEMBLCompounds: delete_after_combining=True but need_combine=False")

    v_logger.UpdateFormat(compounds_config["logger_label"],
                          compounds_config["logger_color"])

    v_logger.info(f"{'-' * 21} ChEMBL downloading for DrugDesign {'-' * 21}")

    os.makedirs(compounds_config["results_folder_name"], exist_ok=True)

    if config["testing_flag"]:
        compounds_config["mw_ranges"] = [[0, 50], [50, 75]]

    for mw_range in compounds_config["mw_ranges"]:
        less_limit = mw_range[0]
        greater_limit = mw_range[1]

        if not config["skip_downloaded"] or\
            not IsFileInFolder(f"range_{less_limit}_{greater_limit}_mw_mols.csv",
                               f"{compounds_config["results_folder_name"]}"):
            DownloadCompoundsByMWRange(
                less_limit,
                greater_limit,
                results_folder_name=compounds_config["results_folder_name"]
            )

        else:
            v_logger.info(f"Molecules with mw in range [{less_limit}, "
                          f"{greater_limit}) is already downloaded, skip.",
                          LogMode.VERBOSELY)

        v_logger.info(f"{'-' * 77}", LogMode.VERBOSELY)

    if compounds_config["need_combining"]:
        CombineCSVInFolder(compounds_config["results_folder_name"],
                           compounds_config["combined_file_name"])

    if compounds_config["delete_after_combining"] and compounds_config["need_combining"]:
        v_logger.info(
            f"Deleting files after combining in "
            f"'{compounds_config["results_folder_name"]}'...")

        DeleteFilesInFolder(compounds_config["results_folder_name"], [
            f"{compounds_config["combined_file_name"]}.csv"])

        v_logger.success(
            f"Deleting files after combining in "
            f"'{compounds_config["results_folder_name"]}'!")

    v_logger.success(f"{'-' * 21} ChEMBL downloading for DrugDesign {'-' * 21}")
    v_logger.info(f"{'-' * 77}")
