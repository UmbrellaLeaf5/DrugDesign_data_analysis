import gdown
import zipfile

from chembl_webresource_client.new_client import new_client
from chembl_webresource_client.query_set import QuerySet

from ChEMBL_download_activities.download import GetCellLineChEMBLActivitiesFromCSV
from ChEMBL_download_activities.functions import CountCellLineActivitiesByFile

from Utils.decorators import ReTry
from Utils.file_and_logger_funcs import IsFolderEmpty, logger, os, pd, UpdateLoggerFormat


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

    return new_client.cell_line.filter(cell_chembl_id__in=cell_line_chembl_id_list)  # type: ignore


def GetRawCellLinesData(file_id: str,
                        output_path: str,
                        print_to_console: bool):
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

    if config["print_to_console_verbosely"]:
        logger.info("Adding 'IC50' and 'GI50' columns to pandas.DataFrame...")

    if IsFolderEmpty(cell_lines_config["raw_csv_folder_name"]):
        if config["print_to_console_verbosely"]:
            logger.info("Getting raw cell_lines from Google.Drive...")

        GetRawCellLinesData(cell_lines_config["raw_csv_g_drive_id"],
                            cell_lines_config["raw_csv_folder_name"],
                            config["print_to_console_verbosely"])

        if config["print_to_console_verbosely"]:
            logger.success("Getting raw cell_lines from Google.Drive!")

    data["IC50"] = data.apply(
        lambda value: CountCellLineActivitiesByFile(
            f"{cell_lines_config["raw_csv_folder_name"]}/{value["cell_chembl_id"]}_IC50_activities.csv"), axis=1)

    data["GI50"] = data.apply(
        lambda value: CountCellLineActivitiesByFile(
            f"{cell_lines_config["raw_csv_folder_name"]}/{value["cell_chembl_id"]}_GI50_activities.csv"), axis=1)

    if config["print_to_console_verbosely"]:
        logger.success("Adding 'IC50' and 'GI50' columns to pandas.DataFrame!")

    if cell_lines_config["download_activities"]:
        GetCellLineChEMBLActivitiesFromCSV(data, config)

        try:
            data["IC50_new"] = data["IC50_new"].astype(int)
            data["GI50_new"] = data["GI50_new"].astype(int)

        except KeyError as exception:  # это исключение может возникнуть, если колонки нет
            # новых activities не скачалось, т.е. значение пересчитывать не надо
            if not config["skip_downloaded"]:
                raise exception

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

    if config["print_to_console_verbosely"]:
        logger.info("Downloading cell_lines...")

    cell_lines_with_ids: QuerySet = QuerySetCellLinesFromIdList(
        cell_lines_config["id_list"])

    if cell_lines_config["id_list"] == []:
        cell_lines_with_ids = QuerySetAllCellLines()

    logger.info(f"Amount: {len(cell_lines_with_ids)}")  # type: ignore

    if config["print_to_console_verbosely"]:
        logger.success("Downloading cell_lines!")

        logger.info("Collecting cell_lines to pandas.DataFrame...")

    data_frame = AddedIC50andGI50ToCellLinesDF(
        pd.DataFrame(cell_lines_with_ids),  # type: ignore
        config)

    UpdateLoggerFormat(cell_lines_config["logger_label"],
                       cell_lines_config["logger_color"])

    if config["print_to_console_verbosely"]:
        logger.success("Collecting cell_lines to pandas.DataFrame!")

        logger.info(
            f"Collecting cell_lines to .csv file in '{cell_lines_config["results_folder_name"]}'...")

    file_name: str = f"{cell_lines_config["results_folder_name"]}/{cell_lines_config["results_file_name"]}.csv"

    data_frame.to_csv(file_name, sep=";", index=False)

    if config["print_to_console_verbosely"]:
        logger.success(
            f"Collecting cell_lines to .csv file in '{cell_lines_config["results_folder_name"]}'!")
