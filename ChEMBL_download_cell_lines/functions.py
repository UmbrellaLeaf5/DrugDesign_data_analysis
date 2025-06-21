"""
ChEMBL_download_cell_lines/functions.py

Этот модуль содержит функции для загрузки и обработки данных о клеточных линиях
из базы данных ChEMBL.
"""

import zipfile

import gdown
from chembl_webresource_client.new_client import new_client
from chembl_webresource_client.query_set import QuerySet

from ChEMBL_download_activities.download import GetCellLineChEMBLActivitiesFromCSV
from ChEMBL_download_activities.functions import CountCellLineActivitiesByFile
from Configurations.config import Config, config
from Utils.decorators import ReTry
from Utils.files_funcs import IsFolderEmpty, os, pd
from Utils.verbose_logger import LogMode, v_logger


@ReTry()
def QuerySetAllCellLines() -> QuerySet:
  """
  Возвращает все клеточные линии из базы ChEMBL.

  Returns:
      QuerySet: набор всех целей
  """

  return new_client.cell_line.filter()  # type: ignore


@ReTry()
def QuerySetCellLinesFromIdList(cell_line_chembl_id_list: list[str]) -> QuerySet:
  """
  Возвращает клеточные линии по списку id из базы ChEMBL.

  Args:
      cell_line_chembl_id_list (list[str]): список id.

  Returns:
      QuerySet: набор целей по списку id.
  """

  return new_client.cell_line.filter(  # type: ignore
    cell_chembl_id__in=cell_line_chembl_id_list
  )


def GetRawCellLinesData(file_id: str, output_path: str, print_to_console: bool):
  """
  Скачивает zip-файл из Google.Drive,
  извлекает его содержимое, а затем удаляет zip-файл.

  Args:
      file_id: ID файла в Google Drive.
      output_path: путь к каталогу, куда будут помещены извлеченные файлы.
      print_to_console (bool): нужно ли выводить логирование в консоль.
  """

  os.makedirs(output_path, exist_ok=True)

  url = f"https://drive.google.com/uc?id={file_id}&export=download"

  zip_file_path = f"{output_path}.zip"
  gdown.download(url, zip_file_path, quiet=(not print_to_console))

  with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
    zip_ref.extractall(output_path)

  os.remove(zip_file_path)


@ReTry(attempts_amount=1)
def AddedIC50andGI50ToCellLinesDF(data: pd.DataFrame) -> pd.DataFrame:
  """
  Добавляет столбцы `IC50` и `GI50` в DataFrame с данными о клеточных линиях,
  подсчитывая количество соответствующих активностей из CSV-файлов,
  а также опционально скачивает новые активности.

  Args:
      data (pd.DataFrame): DataFrame с данными о клеточных линиях.

  Returns:
      pd.DataFrame: DataFrame с добавленными столбцами `IC50` и `GI50`,
                    содержащими количество соответствующих активностей.
  """

  # получаем конфигурацию для клеточных линий.
  cell_lines_config: Config = config["ChEMBL_download_cell_lines"]

  v_logger.info(
    "Adding 'IC50' and 'GI50' columns to pandas.DataFrame...", LogMode.VERBOSELY
  )

  # проверяем, пуста ли папка с необработанными данными.
  if IsFolderEmpty(cell_lines_config["raw_csv_folder_name"]):
    v_logger.info("Getting raw cell_lines from Google.Drive...", LogMode.VERBOSELY)

    GetRawCellLinesData(
      cell_lines_config["raw_csv_g_drive_id"],
      cell_lines_config["raw_csv_folder_name"],
      config["Utils"]["VerboseLogger"]["verbose_print"],
    )

    v_logger.success("Getting raw cell_lines from Google.Drive!", LogMode.VERBOSELY)

  # добавляем столбец 'IC50', подсчитывая активности по файлам.
  data["IC50"] = data.apply(
    lambda value: CountCellLineActivitiesByFile(
      f"{cell_lines_config['raw_csv_folder_name']}/"
      f"{value['cell_chembl_id']}_IC50_activities.csv"
    ),
    axis=1,
  )

  # добавляем столбец 'GI50', подсчитывая активности по файлам.
  data["GI50"] = data.apply(
    lambda value: CountCellLineActivitiesByFile(
      f"{cell_lines_config['raw_csv_folder_name']}/"
      f"{value['cell_chembl_id']}_GI50_activities.csv"
    ),
    axis=1,
  )

  v_logger.success(
    "Adding 'IC50' and 'GI50' columns to pandas.DataFrame!", LogMode.VERBOSELY
  )

  # проверяем, нужно ли скачивать активности.
  if cell_lines_config["download_activities"]:
    GetCellLineChEMBLActivitiesFromCSV(data)

    try:
      # оставляем только строки, в которых есть IC50_new и Ki_new
      data = data[(data["IC50_new"].notna()) & (data["GI50_new"].notna())]

      data = data.copy()

      data["IC50_new"] = data["IC50_new"].astype(int)
      data["GI50_new"] = data["GI50_new"].astype(int)

    # это исключение может возникнуть, если колонки нет.
    except KeyError as exception:
      # новых activities не скачалось, т.е. значение пересчитывать не надо.
      if not config["skip_downloaded"]:
        raise exception

    # это исключение может возникнуть, если какое-то значение оказалось невалидным.
    except pd.errors.IntCastingNaNError:
      v_logger.warning("Cannot convert non-finite values!")

  return data


def DownloadCellLinesFromIdList():
  """
  Скачивает данные о клеточных линиях из ChEMBL по списку идентификаторов,
  добавляет информацию об активностях IC50 и GI50, проводит первичный анализ
  и сохраняет результаты в CSV-файл.
  """

  # получаем конфигурацию для клеточных линий.
  cell_lines_config: Config = config["ChEMBL_download_cell_lines"]

  v_logger.info("Downloading cell_lines...", LogMode.VERBOSELY)

  # получаем клеточные линии по списку id.
  cell_lines_with_ids: QuerySet = QuerySetCellLinesFromIdList(
    cell_lines_config["id_list"]
  )

  # если список id пуст, получаем все клеточные линии.
  if cell_lines_config["id_list"] == []:
    cell_lines_with_ids = QuerySetAllCellLines()

  v_logger.info(f"Amount: {len(cell_lines_with_ids)}")  # type: ignore
  v_logger.success("Downloading cell_lines!", LogMode.VERBOSELY)
  v_logger.info("Collecting cell_lines to pandas.DataFrame...", LogMode.VERBOSELY)

  # добавляем информацию об активностях IC50 и GI50.
  data_frame = AddedIC50andGI50ToCellLinesDF(pd.DataFrame(cell_lines_with_ids))  # type: ignore

  v_logger.UpdateFormat(
    cell_lines_config["logger_label"], cell_lines_config["logger_color"]
  )

  v_logger.success("Collecting cell_lines to pandas.DataFrame!", LogMode.VERBOSELY)
  v_logger.info(
    f"Collecting cell_lines to .csv file in "
    f"'{cell_lines_config['results_folder_name']}'...",
    LogMode.VERBOSELY,
  )

  # формируем имя файла для сохранения.
  file_name: str = (
    f"{cell_lines_config['results_folder_name']}/"
    f"{cell_lines_config['results_file_name']}.csv"
  )

  # сохраняем DataFrame в CSV-файл.
  data_frame.to_csv(file_name, sep=";", index=False)

  v_logger.success(
    f"Collecting cell_lines to .csv file in "
    f"'{cell_lines_config['results_folder_name']}'!",
    LogMode.VERBOSELY,
  )
