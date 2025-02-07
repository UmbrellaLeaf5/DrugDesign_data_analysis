# type: ignore

from chembl_webresource_client.new_client import new_client
from chembl_webresource_client.query_set import QuerySet

from Utils.decorators import Retry
from Utils.primary_analysis import *

from ChEMBL_download_activities.download import GetCellLineChEMBLActivitiesFromCSV
from ChEMBL_download_activities.functions import CountCellLineActivitiesByFile


@Retry()
def QuerySetAllCellLines() -> QuerySet:
    """
    Возвращает все цели из базы ChEMBL.

    Returns:
        QuerySet: набор всех целей
    """

    return new_client.cell_line.filter()


@Retry()
def QuerySetCellLinesFromIdList(cell_line_chembl_id_list: list[str]) -> QuerySet:
    """
    Возвращает цели по списку id из базы ChEMBL.

    Args:
        cell_line_chembl_id_list (list[str]): список id

    Returns:
        QuerySet: набор целей по списку id
    """

    return new_client.cell_line.filter(cell_chembl_id__in=cell_line_chembl_id_list)


def AddedIC50andGI50ToCellLinesDF(data: pd.DataFrame,
                                  get_activities: bool = True,
                                  raw_csv_folder_name: str = "raw/cell_lines_activities",
                                  activities_results_folder_name: str = "results/activities",
                                  download_compounds_sdf: bool = True,
                                  print_to_console: bool = False,
                                  skip_gotten_activities: bool = False) -> pd.DataFrame:
    """
    Добавляет в pd.DataFrame два столбца: IC50 и GI50.

    Args:
        data (pd.DataFrame): исходный pd.DataFrame
        need_to_download_activities (bool, optional): нужно ли получать activities отдельно. Defaults to True.
        raw_csv_folder_name (str, optional): название папки, откуда необходимо получить activities. Defaults to "raw/cell_lines_activities".
        activities_results_folder_name (str, optional): название папки для полученных activities. Defaults to "results/activities".
        download_compounds_sdf (bool, optional): нужно ли скачивать .sdf файл с molfile для каждой молекулы. Defaults to True.
        print_to_console (bool, optional): нужно ли выводить логирование в консоль. Defaults to False.
        skip_gotten_activities (bool, optional): пропускать ли уже скачанные файлы activities. Defaults to False.

    Returns:
        pd.DataFrame: расширенный pd.DataFrame
    """

    logger.info(
        f"Adding 'IC50' and 'GI50' columns to pandas.DataFrame()...".ljust(77))

    try:
        data["IC50"] = data.apply(
            lambda value: CountCellLineActivitiesByFile(
                f"{raw_csv_folder_name}/{value["cell_chembl_id"]}_IC50_activities.csv"), axis=1)
        data["GI50"] = data.apply(
            lambda value: CountCellLineActivitiesByFile(
                f"{raw_csv_folder_name}/{value["cell_chembl_id"]}_GI50_activities.csv"), axis=1)

        logger.success(
            f"Adding 'IC50' and 'GI50' columns to pandas.DataFrame(): SUCCESS".ljust(77))

        if get_activities:
            GetCellLineChEMBLActivitiesFromCSV(data,
                                               raw_csv_folder_name=raw_csv_folder_name,
                                               results_folder_name=activities_results_folder_name,
                                               download_compounds_sdf=download_compounds_sdf,
                                               print_to_console=print_to_console,
                                               skip_gotten_activities=skip_gotten_activities)
            try:
                data["IC50_new"] = data["IC50_new"].astype(int)
                data["GI50_new"] = data["GI50_new"].astype(int)

            except Exception as exception:  # это исключение может возникнуть, если колонки нет
                if not skip_gotten_activities:  # новых activities не скачалось, т.е. значение пересчитывать не надо
                    raise exception

                else:
                    pass

    except Exception as exception:
        PrintException(exception, "ChEMBL____cells", "fg #CB507C")

    return data


def DownloadCellLinesFromIdList(cell_line_chembl_id_list: list[str] = [],
                                results_folder_name: str = "results/cell_lines",
                                primary_analysis_folder_name: str = "primary_analysis",
                                need_primary_analysis: bool = False,
                                get_activities: bool = True,
                                activities_results_folder_name: str = "results/activities",
                                print_to_console: bool = False,
                                skip_gotten_activities: bool = False) -> None:
    """
    Скачивает клеточные линии по списку id из базы ChEMBL, сохраняя их в .csv файл.

    Args:
        cell_line_chembl_id_list (list[str], optional): список id. Defaults to []: для скачивания всех клеточных линий.
        results_folder_name (str, optional): имя папки для закачки. Defaults to "results/cell_lines".
        primary_analysis_folder_name (str, optional): имя папки для сохранения данных о первичном анализе. Defaults to "primary_analysis".
        need_primary_analysis (bool, optional): нужно ли проводить первичный анализ. Defaults to False.
        get_activities (bool, optional): нужно ли получать активности к клеточным линиям по IC50 и GI50. Defaults to True.
        activities_results_folder_name (str, optional): имя папки для закачки activities. Defaults to "results/activities".
        print_to_console (bool, optional): нужно ли выводить логирование в консоль. Defaults to False.
        skip_gotten_activities (bool, optional): пропускать ли уже скачанные файлы activities. Defaults to False.
    """

    try:
        logger.info(
            f"Downloading cell lines...".ljust(77))
        cell_lines_with_ids: QuerySet = QuerySetCellLinesFromIdList(
            cell_line_chembl_id_list)

        if cell_line_chembl_id_list == []:
            cell_lines_with_ids = QuerySetAllCellLines()

        logger.info(f"Amount: {len(cell_lines_with_ids)}".ljust(77))
        logger.success(f"Downloading cell lines: SUCCESS".ljust(77))

        try:
            logger.info(
                "Collecting cell lines to pandas.DataFrame()...".ljust(77))

            data_frame = AddedIC50andGI50ToCellLinesDF(
                pd.DataFrame(cell_lines_with_ids),
                get_activities=get_activities,
                activities_results_folder_name=activities_results_folder_name,
                print_to_console=print_to_console,
                skip_gotten_activities=skip_gotten_activities)

            UpdateLoggerFormat("ChEMBL____cells", "fg #CB507C")

            logger.success(
                "Collecting cell lines to pandas.DataFrame(): SUCCESS".ljust(77))

            logger.info(
                f"Collecting cell lines to .csv file in '{results_folder_name}'...".ljust(77))

            if need_primary_analysis:
                PrimaryAnalysisByColumns(data_frame=data_frame,
                                         data_name=f"cell_lines_data_from_ChEMBL",
                                         folder_name=f"{
                                             results_folder_name}/{primary_analysis_folder_name}",
                                         print_to_console=print_to_console)

                UpdateLoggerFormat("ChEMBL____cells", "fg #CB507C")

            file_name: str = f"{
                results_folder_name}/cell_lines_data_from_ChEMBL.csv"

            data_frame.to_csv(file_name, sep=';', index=False)
            logger.success(
                f"Collecting cell lines to .csv file in '{results_folder_name}': SUCCESS".ljust(77))

        except Exception as exception:
            PrintException(exception, "ChEMBL____cells", "fg #CB507C")

    except Exception as exception:
        PrintException(exception, "ChEMBL____cells", "fg #CB507C")
