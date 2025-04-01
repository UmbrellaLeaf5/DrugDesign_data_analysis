"""
ChEMBL_download_targets/download.py

Этот модуль отвечает за скачивание информации о мишенях из базы данных
ChEMBL, используя конфигурацию из `config.json`.
"""

from ChEMBL_download_targets.functions import *

from Utils.decorators import IgnoreWarnings
from Utils.files_funcs import IsFileInFolder, os
from Utils.verbose_logger import LogMode

from Configurations.config import config, Config


@IgnoreWarnings
def DownloadChEMBLTargets():
  """
  Скачиваем информацию о мишенях из базы данных ChEMBL на основе
  конфигурации (`config.json`).
  """

  # получаем конфигурацию для скачивания мишеней.
  targets_config: Config = config["ChEMBL_download_targets"]
  # получаем конфигурацию для скачивания активностей.
  activities_config: Config = config["ChEMBL_download_activities"]

  v_logger.UpdateFormat(targets_config["logger_label"],
                        targets_config["logger_color"])

  v_logger.info(f"{"• " * 10} ChEMBL downloading for DrugDesign.")

  # создаем директорию для результатов скачивания, если она не существует.
  os.makedirs(targets_config["results_folder_name"], exist_ok=True)

  # если нужно скачивать активности, создаем директорию для активностей.
  if targets_config["download_activities"]:
    os.makedirs(activities_config["results_folder_name"], exist_ok=True)

  # если установлен флаг тестирования, используем ограниченный список id.
  if config["testing_flag"]:
    targets_config["id_list"] = ["CHEMBL1951", "CHEMBL2034"]

  # если файлы не скачаны или их нет в папке.
  if not config["skip_downloaded"] or not IsFileInFolder(
          targets_config["results_file_name"],
          targets_config["results_folder_name"]):
    # если скачиваем все мишени, очищаем список id.
    if targets_config["download_all"]:
      targets_config["id_list"] = []

    # скачиваем данные о мишенях.
    DownloadTargetsFromIdList()

  # если файлы уже скачаны, пропускаем.
  else:
    v_logger.info(
        f"{targets_config["results_file_name"]} is already "
        f"downloaded, skip",
        LogMode.VERBOSELY)

  v_logger.info(f"{"• " * 10} ChEMBL downloading for DrugDesign!")
  v_logger.info()
