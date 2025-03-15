"""
ChEMBL_download_compounds/download.py

Этот модуль отвечает за скачивание данных о соединениях из базы данных
ChEMBL и их объединение.
"""

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
    Скачивает информацию о соединениях из базы данных ChEMBL на основе
    конфигурации (`config.json`).
    """

    # получаем конфигурацию для скачивания соединений.
    compounds_config: Config = config["ChEMBL_download_compounds"]

    # проверяем, что если нужно удалять файлы после объединения, то и
    # объединение должно быть включено.
    if compounds_config["delete_after_combining"] and \
            not compounds_config["need_combining"]:
        raise ValueError(
            "DownloadChEMBLCompounds: delete_after_combining=True but "
            "need_combine=False")

    v_logger.UpdateFormat(compounds_config["logger_label"],
                          compounds_config["logger_color"])

    v_logger.info(f"{"• " * 10} ChEMBL downloading for DrugDesign.")

    # создаем директорию для результатов, если она не существует.
    os.makedirs(compounds_config["results_folder_name"], exist_ok=True)

    # если установлен флаг тестирования, ограничиваем диапазоны молекулярных масс.
    if config["testing_flag"]:
        compounds_config["mw_ranges"] = [[0, 50], [50, 75]]

    # итерируемся по диапазонам молекулярных масс.
    for mw_range in compounds_config["mw_ranges"]:
        less_limit = mw_range[0]
        greater_limit = mw_range[1]

        # если не нужно пропускать скачанные или файл не существует.
        if not config["skip_downloaded"] or not IsFileInFolder(
                f"range_{less_limit}_{greater_limit}_mw_mols.csv",
                f"{compounds_config["results_folder_name"]}"):
            # скачиваем соединения для текущего диапазона.
            DownloadCompoundsByMWRange(
                less_limit,
                greater_limit,
                results_folder_name=compounds_config["results_folder_name"]
            )

        # если файл уже скачан, пропускаем.
        else:
            v_logger.info(
                f"Molecules with mw in range [{less_limit}, "
                f"{greater_limit}) is already downloaded, skip.",
                LogMode.VERBOSELY)

        v_logger.info("-", LogMode.VERBOSELY)

    # если нужно объединять файлы.
    if compounds_config["need_combining"]:
        # объединяем CSV файлы в папке.
        CombineCSVInFolder(compounds_config["results_folder_name"],
                           compounds_config["combined_file_name"])

    # если нужно удалять файлы после объединения и объединение включено.
    if compounds_config["delete_after_combining"] and \
            compounds_config["need_combining"]:
        v_logger.info(
            f"Deleting files after combining in "
            f"'{compounds_config["results_folder_name"]}'...")

        # удаляем файлы, кроме объединенного.
        DeleteFilesInFolder(compounds_config["results_folder_name"], [
            f"{compounds_config["combined_file_name"]}.csv"])

        v_logger.success(
            f"Deleting files after combining in "
            f"'{compounds_config["results_folder_name"]}'!")

    v_logger.success(f"{"• " * 10} ChEMBL downloading for DrugDesign!")
    v_logger.info()
