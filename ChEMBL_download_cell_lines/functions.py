from chembl_webresource_client.new_client import new_client
from chembl_webresource_client.query_set import QuerySet

from Utils.decorators import Retry
from Utils.primary_analysis import *

from ChEMBL_download_activities.download import GetCellLineChEMBLActivitiesFromCSV
from ChEMBL_download_activities.functions import CountCellLineActivitiesByFile

import gdown
import zipfile
import os


@Retry()
def QuerySetAllCellLines() -> QuerySet:
    """
    Возвращает все клеточные линии из базы ChEMBL.

    Returns:
        QuerySet: набор всех целей
    """

    return new_client.cell_line.filter()  # type: ignore


@Retry()
def QuerySetCellLinesFromIdList(cell_line_chembl_id_list: list[str]) -> QuerySet:
    """
    Возвращает клеточные линии по списку id из базы ChEMBL.

    Args:
        cell_line_chembl_id_list (list[str]): список id.

    Returns:
        QuerySet: набор целей по списку id.
    """

    return new_client.cell_line.filter(cell_chembl_id__in=cell_line_chembl_id_list)  # type: ignore


def GetRawCellLinesData(file_id: str, output_path: str):
    """
    Скачивает zip-файл из Google.Drive,
    извлекает его содержимое, а затем удаляет zip-файл.

    Args:
        file_id: ID файла в Google Drive.
        output_path: путь к каталогу, куда будут помещены извлеченные файлы.
    """

    os.makedirs(output_path, exist_ok=True)

    url = f"https://drive.google.com/uc?id={file_id}&export=download"

    zip_file_path = f"{output_path}.zip"
    gdown.download(url, zip_file_path, quiet=False)

    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        zip_ref.extractall(output_path)

    os.remove(zip_file_path)


def AddedIC50andGI50ToCellLinesDF(data: pd.DataFrame,
                                  config: dict
                                  ) -> pd.DataFrame:
    """
    Добавляет столбцы `IC50` и `GI50` в DataFrame с данными о клеточных линиях, подсчитывая
    количество соответствующих активностей из CSV-файлов, а также опционально скачивает новые активности.

    Args:
        data (pd.DataFrame): DataFrame с данными о клеточных линиях.
        config (dict): словарь, содержащий параметры конфигурации для процесса скачивания.

    Returns:
        pd.DataFrame: DataFrame с добавленными столбцами `IC50` и `GI50`,
                      содержащими количество соответствующих активностей.
    """

    cell_lines_config: dict = config["ChEMBL_download_cell_lines"]

    logger.info(
        f"Adding 'IC50' and 'GI50' columns to pandas.DataFrame()...")

    try:
        logger.info(
            f"Getting raw cell_lines from Google.Drive...")
        GetRawCellLinesData(config["ChEMBL_download_cell_lines"]["raw_csv_g_drive_id"],
                            config["ChEMBL_download_cell_lines"]["raw_csv_folder_name"])
        logger.success(
            f"Getting raw cell_lines from Google.Drive!")

        data["IC50"] = data.apply(
            lambda value: CountCellLineActivitiesByFile(
                f"{cell_lines_config["raw_csv_folder_name"]}/{value["cell_chembl_id"]}_IC50_activities.csv"), axis=1)

        data["GI50"] = data.apply(
            lambda value: CountCellLineActivitiesByFile(
                f"{cell_lines_config["raw_csv_folder_name"]}/{value["cell_chembl_id"]}_GI50_activities.csv"), axis=1)

        logger.success(
            f"Adding 'IC50' and 'GI50' columns to pandas.DataFrame()!")

        if cell_lines_config["download_activities"]:
            GetCellLineChEMBLActivitiesFromCSV(data, config)

            try:
                data["IC50_new"] = data["IC50_new"].astype(int)
                data["GI50_new"] = data["GI50_new"].astype(int)

            except Exception as exception:  # это исключение может возникнуть, если колонки нет
                # новых activities не скачалось, т.е. значение пересчитывать не надо
                if not config["skip_downloaded"]:
                    raise exception
                else:
                    pass

    except Exception as exception:
        LogException(exception)

    return data


def DownloadCellLinesFromIdList(config: dict):
    """
    Скачивает данные о клеточных линиях из ChEMBL по списку идентификаторов,
    добавляет информацию об активностях IC50 и GI50, проводит первичный анализ
    и сохраняет результаты в CSV-файл.

    Args:
        config (dict): словарь, содержащий параметры конфигурации для процесса скачивания.
    """

    cell_lines_config: dict = config["ChEMBL_download_cell_lines"]

    print_to_console = (
        config["print_to_console_verbosely"] or config["testing_flag"])

    try:
        logger.info(
            f"Downloading cell lines...")
        cell_lines_with_ids: QuerySet = QuerySetCellLinesFromIdList(
            cell_lines_config["id_list"])

        if cell_lines_config["id_list"] == []:
            cell_lines_with_ids = QuerySetAllCellLines()

        logger.info(f"Amount: {len(cell_lines_with_ids)}")  # type: ignore
        logger.success(f"Downloading cell lines!")

        try:
            logger.info(
                "Collecting cell lines to pandas.DataFrame()...")

            data_frame = AddedIC50andGI50ToCellLinesDF(pd.DataFrame(cell_lines_with_ids),  # type: ignore
                                                       config=config)

            UpdateLoggerFormat(cell_lines_config["logger_label"], cell_lines_config["logger_color"])

            logger.success(
                "Collecting cell lines to pandas.DataFrame()!")

            logger.info(
                f"Collecting cell lines to .csv file in '{cell_lines_config["results_folder_name"]}'...")

            if config["need_primary_analysis"]:
                PrimaryAnalysisByColumns(data_frame=data_frame,
                                         data_name=cell_lines_config["results_file_name"],
                                         folder_name=f"{cell_lines_config["results_folder_name"]}/{config["primary_analysis_folder_name"]}",
                                         print_to_console=print_to_console)

                UpdateLoggerFormat(cell_lines_config["logger_label"],
                                   cell_lines_config["logger_color"])

            file_name: str = f"{cell_lines_config["results_folder_name"]}/{cell_lines_config["results_file_name"]}.csv"

            data_frame.to_csv(file_name, sep=';', index=False)
            logger.success(
                f"Collecting cell lines to .csv file in '{cell_lines_config["results_folder_name"]}'!")

        except Exception as exception:
            LogException(exception)

    except Exception as exception:
        LogException(exception)
