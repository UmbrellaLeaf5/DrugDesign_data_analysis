"""
ChEMBL_download_cell_lines/download.py

Этот модуль отвечает за скачивание данных о клеточных линиях из базы данных
ChEMBL и сохранение их в файлы.
"""

from ChEMBL_download_cell_lines.functions import *
from Configurations.config import Config, config
from Utils.decorators import IgnoreWarnings
from Utils.files_funcs import IsFileInFolder, os
from Utils.verbose_logger import LogMode


@IgnoreWarnings
def DownloadChEMBLCellLines():
  """
  Скачивает информацию о клеточных линиях из базы данных ChEMBL на основе
  конфигурации (`config.json`).
  """

  # конфигурация для скачивания клеточных линий.
  cell_lines_config: Config = config["ChEMBL_download_cell_lines"]
  # конфигурация для скачивания активностей.
  activities_config: Config = config["ChEMBL_download_activities"]

  v_logger.UpdateFormat(
    cell_lines_config["logger_label"], cell_lines_config["logger_color"]
  )

  v_logger.info(f"{'• ' * 10} ChEMBL downloading for DrugDesign.")

  # создаем директорию для сохранения результатов, если она не существует.
  os.makedirs(cell_lines_config["results_folder_name"], exist_ok=True)

  # если нужно скачивать активности, создаем и для них директорию.
  if cell_lines_config["download_activities"]:
    os.makedirs(activities_config["results_folder_name"], exist_ok=True)

  # если установлен флаг тестирования, ограничиваем список id.
  if config["testing_flag"]:
    cell_lines_config["id_list"] = ["CHEMBL4295386", "CHEMBL3307781"]

  # если не нужно пропускать скачанные или файл не существует.
  if not config["skip_downloaded"] or not IsFileInFolder(
    f"{cell_lines_config['results_file_name']}.csv",
    f"{cell_lines_config['results_folder_name']}",
  ):
    # если нужно скачивать все, очищаем список id (скачаются все).
    if cell_lines_config["download_all"]:
      cell_lines_config["id_list"] = []

    DownloadCellLinesFromIdList()

  # если файл уже скачан, пропускаем.
  else:
    v_logger.info(
      f"{cell_lines_config['results_file_name']} is already downloaded, skip.",
      LogMode.VERBOSELY,
    )

  v_logger.success(f"{'• ' * 10} ChEMBL downloading for DrugDesign!")
  v_logger.info()
