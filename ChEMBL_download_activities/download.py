"""
ChEMBL_download_activities/download.py

Этот модуль содержит функции для скачивания и обработки данных об активностях,
связанных с мишенями и клеточными линиями из базы данных ChEMBL,
а также для сохранения молекулярных файлов (molfiles) в формате SDF.
"""

from ChEMBL_download_activities.functions import *
from ChEMBL_download_compounds.functions import SaveChEMBLMolfilesToSDFByIdList
from Configurations.config import Config, config
from Utils.decorators import IgnoreWarnings, ReTry
from Utils.files_funcs import IsFileInFolder, os
from Utils.verbose_logger import LogMode, v_logger


@IgnoreWarnings
@ReTry(attempts_amount=1)
def DownloadTargetChEMBLActivities(targets_data: pd.DataFrame):
  """
  Скачивает информацию об активностях (IC50 и Ki), связанных с заданными мишенями,
  из базы данных ChEMBL и сохраняет их в CSV-файлы.

  Также, при необходимости, скачивает соответствующие molfiles в формате SDF.

  Args:
      targets_data (pd.DataFrame): DataFrame, содержащий информацию о мишенях,
                                   включая 'target_chembl_id'.
  """

  # конфигурация для скачивания активностей.
  activities_config: Config = config["ChEMBL_download_activities"]
  # конфигурация для скачивания соединений.
  compounds_config: Config = config["ChEMBL_download_compounds"]

  v_logger.UpdateFormat(
    activities_config["logger_label"], activities_config["logger_color"]
  )

  v_logger.info("Start download activities connected with targets...")
  v_logger.info("-", LogMode.VERBOSELY)

  # итерируемся по идентификаторам мишеней.
  for target_id in targets_data["target_chembl_id"]:
    file_name_ic50: str = f"{target_id}_IC50_activities"
    file_name_ki: str = f"{target_id}_Ki_activities"

    # нужно ли пропускать скачивание, если файлы уже существуют.
    if (
      config["skip_downloaded"]
      and IsFileInFolder(
        f"{file_name_ic50}.csv", activities_config["results_folder_name"]
      )
      and IsFileInFolder(f"{file_name_ki}.csv", activities_config["results_folder_name"])
    ):
      v_logger.info(
        f"Activities connected with target {target_id} is already downloaded, skip.",
        LogMode.VERBOSELY,
      )
      v_logger.info("-", LogMode.VERBOSELY)

      continue  # переходим к следующей мишени.

    v_logger.info(
      f"Downloading activities connected with {target_id}...", LogMode.VERBOSELY
    )

    # активности IC50 для мишени.
    activities_ic50: QuerySet = QuerySetActivitiesByIC50(target_id)
    # активности Ki для мишени.
    activities_ki: QuerySet = QuerySetActivitiesByKi(target_id)

    v_logger.info(
      "Amount: IC50: "
      f"{len(activities_ic50)};"  # type: ignore
      " Ki: "
      f"{len(activities_ki)}.",  # type: ignore
      LogMode.VERBOSELY,
    )
    v_logger.success(
      f"Downloading activities connected with {target_id}!", LogMode.VERBOSELY
    )
    v_logger.info("Collecting activities to pandas.DataFrame...", LogMode.VERBOSELY)

    # очищаем DataFrame с активностями IC50.
    data_frame_ic50 = CleanedTargetActivitiesDF(
      pd.DataFrame(activities_ic50),  # type: ignore
      target_id=target_id,
      activities_type="IC50",
    )

    # очищаем DataFrame с активностями Ki.
    data_frame_ki = CleanedTargetActivitiesDF(
      pd.DataFrame(activities_ki),  # type: ignore
      target_id=target_id,
      activities_type="Ki",
    )

    v_logger.success("Collecting activities to pandas.DataFrame!", LogMode.VERBOSELY)
    v_logger.info(
      "Recording new values 'IC50', 'Ki' in targets DataFrame...", LogMode.VERBOSELY
    )

    # записываем количество активностей IC50 и Ki в DataFrame с данными о мишенях.
    targets_data.loc[targets_data["target_chembl_id"] == target_id, "IC50_new"] = len(
      data_frame_ic50
    )

    targets_data.loc[targets_data["target_chembl_id"] == target_id, "Ki_new"] = len(
      data_frame_ki
    )

    v_logger.info(
      f"Amount: IC50: {len(data_frame_ic50)}; Ki: {len(data_frame_ki)}.",
      LogMode.VERBOSELY,
    )
    v_logger.success(
      "Recording new values 'IC50', 'Ki' in targets DataFrame!", LogMode.VERBOSELY
    )
    v_logger.info(
      "Collecting activities to .csv file in "
      f"'{activities_config['results_folder_name']}'...",
      LogMode.VERBOSELY,
    )

    # полное имя файла для IC50.
    full_file_name_ic50: str = (
      f"{activities_config['results_folder_name']}/{file_name_ic50}.csv"
    )
    # полное имя файла для Ki.
    full_file_name_ki: str = (
      f"{activities_config['results_folder_name']}/{file_name_ki}.csv"
    )

    # сохраняем DataFrame с активностями IC50 в CSV.
    data_frame_ic50.to_csv(full_file_name_ic50, sep=";", index=False)
    # сохраняем DataFrame с активностями Ki в CSV.
    data_frame_ki.to_csv(full_file_name_ki, sep=";", index=False)

    v_logger.success(
      "Collecting activities to .csv file in "
      f"'{activities_config['results_folder_name']}'!",
      LogMode.VERBOSELY,
    )

    # включена опция скачивания molfiles.
    if activities_config["download_compounds_sdf"]:
      # обновляем формат логгера.
      v_logger.UpdateFormat(
        compounds_config["logger_label"], compounds_config["logger_color"]
      )

      v_logger.info(
        f"Start download molfiles connected with {target_id} to .sdf...",
        LogMode.VERBOSELY,
      )

      # создаем директорию для molfiles, если она не существует.
      os.makedirs(compounds_config["molfiles_folder_name"], exist_ok=True)

      v_logger.info("Saving connected with IC50 molfiles...", LogMode.VERBOSELY)

      # сохраняем molfiles, связанные с активностями IC50 в SDF.
      SaveChEMBLMolfilesToSDFByIdList(
        data_frame_ic50["molecule_chembl_id"].tolist(),
        f"{compounds_config['molfiles_folder_name']}/{file_name_ic50}_molfiles",
        extra_data=data_frame_ic50,
      )

      v_logger.success("Saving connected with IC50 molfiles!", LogMode.VERBOSELY)
      v_logger.info("Saving connected with Ki molfiles...", LogMode.VERBOSELY)

      # сохраняем molfiles, связанные с активностями Ki в SDF.
      SaveChEMBLMolfilesToSDFByIdList(
        data_frame_ki["molecule_chembl_id"].tolist(),
        f"{compounds_config['molfiles_folder_name']}/{file_name_ki}_molfiles",
        extra_data=data_frame_ki,
      )

      v_logger.success("Saving connected with Ki molfiles!", LogMode.VERBOSELY)
      v_logger.success(
        f"End download molfiles connected with {target_id} to .sdf!", LogMode.VERBOSELY
      )

      # восстанавливаем формат логгера.
      v_logger.UpdateFormat(
        activities_config["logger_label"], activities_config["logger_color"]
      )

    v_logger.info("-", LogMode.VERBOSELY)

  v_logger.success("End download activities connected with targets!")


@IgnoreWarnings
@ReTry(attempts_amount=1)
def GetCellLineChEMBLActivitiesFromCSV(cell_lines_data: pd.DataFrame):
  """
  "Скачивает" (получает) информацию об активностях (IC50 и GI50), связанных с
  заданными клеточными линиями, из CSV-файлов, расположенных в директории,
  указанной в конфигурации.  Также, при необходимости, скачивает
  соответствующие molfiles в формате SDF.

  Важно:
      В данном случае "скачивание" подразумевает чтение данных из локальных
      CSV-файлов, а не загрузку из ChEMBL API.

  Args:
      cell_lines_data (pd.DataFrame): DataFrame, содержащий информацию
                                       о клеточных линиях, включая 'cell_chembl_id'.
  """

  # конфигурация для активностей.
  activities_config: Config = config["ChEMBL_download_activities"]
  # конфигурация для клеточных линий.
  cell_lines_config: Config = config["ChEMBL_download_cell_lines"]
  # конфигурация для соединений.
  compounds_config: Config = config["ChEMBL_download_compounds"]

  # сохраняем текущий индекс формата логгера.
  restore_index: int = (
    v_logger.UpdateFormat(
      activities_config["logger_label"], activities_config["logger_color"]
    )
    - 1
  )

  v_logger.info("Start getting activities connected with cell_lines...")
  v_logger.info("-", LogMode.VERBOSELY)

  # итерируемся по идентификаторам клеточных линий.
  for cell_id in cell_lines_data["cell_chembl_id"]:
    file_name_ic50: str = f"{cell_id}_IC50_activities"
    file_name_gi50: str = f"{cell_id}_GI50_activities"

    # нужно ли пропускать загрузку, если файлы уже существуют.
    if (
      config["skip_downloaded"]
      and IsFileInFolder(
        f"{file_name_ic50}.csv", activities_config["results_folder_name"]
      )
      and IsFileInFolder(
        f"{file_name_gi50}.csv", activities_config["results_folder_name"]
      )
    ):
      v_logger.info(
        f"Activities connected with target {cell_id} is already gotten, skip",
        LogMode.VERBOSELY,
      )
      v_logger.info("-", LogMode.VERBOSELY)

      continue  # переходим к следующей клеточной линии.

    v_logger.info(f"Getting activities connected with {cell_id}...", LogMode.VERBOSELY)

    # читаем данные об активностях IC50 и GI50 из CSV-файлов.
    data_frame_ic50 = pd.read_csv(
      f"{cell_lines_config['raw_csv_folder_name']}/{file_name_ic50}.csv",
      sep=config["csv_separator"],
      low_memory=False,
    )

    data_frame_gi50 = pd.read_csv(
      f"{cell_lines_config['raw_csv_folder_name']}/{file_name_gi50}.csv",
      sep=config["csv_separator"],
      low_memory=False,
    )

    v_logger.info(
      f"Amount: IC50: {len(data_frame_ic50)}; GI50: {len(data_frame_gi50)}.",
      LogMode.VERBOSELY,
    )

    v_logger.success(f"Getting activities connected with {cell_id}!", LogMode.VERBOSELY)
    v_logger.info("Cleaning activities...", LogMode.VERBOSELY)

    # очищаем DataFrames с активностями IC50 и GI50.
    data_frame_ic50 = CleanedCellLineActivitiesDF(
      data_frame_ic50, cell_id=cell_id, activities_type="IC50"
    )

    data_frame_gi50 = CleanedCellLineActivitiesDF(
      data_frame_gi50, cell_id=cell_id, activities_type="GI50"
    )

    v_logger.success("Collecting activities to pandas.DataFrame!", LogMode.VERBOSELY)
    v_logger.info(
      "Recording new values 'IC50', 'GI50' in targets DataFrame...", LogMode.VERBOSELY
    )

    # записываем количество активностей IC50 и GI50 в DataFrame.
    cell_lines_data.loc[cell_lines_data["cell_chembl_id"] == cell_id, "IC50_new"] = len(
      data_frame_ic50
    )

    cell_lines_data.loc[cell_lines_data["cell_chembl_id"] == cell_id, "GI50_new"] = len(
      data_frame_gi50
    )

    v_logger.info(
      f"Amount: IC50: {len(data_frame_ic50)}; GI50: {len(data_frame_gi50)}.",
      LogMode.VERBOSELY,
    )
    v_logger.success(
      "Recording new values 'IC50', 'GI50' in targets DataFrame!", LogMode.VERBOSELY
    )
    v_logger.info(
      f"Collecting activities to .csv file in '"
      f"{activities_config['results_folder_name']}'...",
      LogMode.VERBOSELY,
    )

    # формируем полное имя файла для IC50.
    full_file_name_ic50: str = (
      f"{activities_config['results_folder_name']}/{file_name_ic50}.csv"
    )
    # формируем полное имя файла для GI50.
    full_file_name_gi50: str = (
      f"{activities_config['results_folder_name']}/{file_name_gi50}.csv"
    )

    # сохраняем DataFrame с активностями IC50 в CSV.
    data_frame_ic50.to_csv(full_file_name_ic50, sep=";", index=False)
    # сохраняем DataFrame с активностями GI50 в CSV.
    data_frame_gi50.to_csv(full_file_name_gi50, sep=";", index=False)

    v_logger.success(
      f"Collecting activities to .csv file in "
      f"'{activities_config['results_folder_name']}'!",
      LogMode.VERBOSELY,
    )

    # включена опция скачивания molfiles.
    if activities_config["download_compounds_sdf"]:
      # обновляем формат логгера.
      v_logger.UpdateFormat(
        compounds_config["logger_label"], compounds_config["logger_color"]
      )

      v_logger.info(
        f"Start download molfiles connected with {cell_id} to .sdf...", LogMode.VERBOSELY
      )

      # создаем директорию для molfiles, если она не существует.
      os.makedirs(compounds_config["molfiles_folder_name"], exist_ok=True)

      v_logger.info("Saving connected with IC50 molfiles...", LogMode.VERBOSELY)

      # сохраняем molfiles, связанные с активностями IC50 в SDF.
      SaveChEMBLMolfilesToSDFByIdList(
        data_frame_ic50["molecule_chembl_id"].tolist(),
        f"{compounds_config['molfiles_folder_name']}/{file_name_ic50}_molfiles",
        extra_data=data_frame_ic50,
      )

      v_logger.success("Saving connected with IC50 molfiles!", LogMode.VERBOSELY)
      v_logger.info("Saving connected with GI50 molfiles...", LogMode.VERBOSELY)

      # сохраняем molfiles, связанные с активностями GI50 в SDF.
      SaveChEMBLMolfilesToSDFByIdList(
        data_frame_gi50["molecule_chembl_id"].tolist(),
        f"{compounds_config['molfiles_folder_name']}/{file_name_gi50}_molfiles",
        extra_data=data_frame_gi50,
      )

      v_logger.success("Saving connected with GI50 molfiles!", LogMode.VERBOSELY)
      v_logger.success(
        f"End download molfiles connected with {cell_id} to .sdf!", LogMode.VERBOSELY
      )

      # восстанавливаем формат логгера.
      v_logger.UpdateFormat(
        activities_config["logger_label"], activities_config["logger_color"]
      )

    v_logger.info("-", LogMode.VERBOSELY)

  v_logger.success("End getting activities connected with cell_lines!")

  # восстанавливаем исходный формат логгера.
  v_logger.RestoreFormat(restore_index)
