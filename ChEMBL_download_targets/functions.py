# type: ignore

from chembl_webresource_client.new_client import new_client
from chembl_webresource_client.query_set import QuerySet

from Utils.decorators import Retry
from Utils.primary_analysis import *

from ChEMBL_download_activities.download import DownloadChEMBLActivities
from ChEMBL_download_activities.functions import CountActivitiesByIC50, CountActivitiesByKi


@Retry()
def QuerySetAllTargets() -> QuerySet:
    """
    Возвращает все цели из базы ChEMBL

    Returns:
        QuerySet: набор всех целей
    """

    return new_client.target.filter()


@Retry()
def QuerySetTargetsFromIdList(target_chembl_id_list: list[str]) -> QuerySet:
    """
    Возвращает цели по списку id из базы ChEMBL

    Args:
        target_chembl_id_list (list[str]): список id

    Returns:
        QuerySet: набор целей по списку id
    """

    return new_client.target.filter(target_chembl_id__in=target_chembl_id_list)


def ExpandedFromDictionariesTargetsDF(data: pd.DataFrame) -> pd.DataFrame:
    """
    Избавляет pd.DataFrame от словарей и списков словарей в столбцах, разбивая их на подстолбцы

    Args:
        data (pd.DataFrame): исходный pd.DataFrame

    Returns:
        pd.DataFrame: "раскрытый" pd.DataFrame
    """

    logger.info(f"Expand pandas.DataFrame() from dictionaries...".ljust(77))

    def ExtractedValuesFromColumn(df: pd.DataFrame, column_name: str, key: str) -> pd.Series:
        return df[column_name].apply(lambda x: [d[key] for d in x] if x else [])

    exposed_data = pd.DataFrame({
        #! cross_references
        'xref_id':                                ExtractedValuesFromColumn(data, 'cross_references', 'xref_id'),
        'xref_name':                              ExtractedValuesFromColumn(data, 'cross_references', 'xref_name'),
        'xref_src':                               ExtractedValuesFromColumn(data, 'cross_references', 'xref_src'),
    })

    # избавлюсь от списков, так как в них находятся одиночные словари
    data['target_components'] = data['target_components'].apply(
        lambda x: x[0] if x else {'accession': None,
                                  'component_description': None,
                                  'component_id': None,
                                  'component_type': None,
                                  'relationship': None,
                                  'target_component_synonyms': [],
                                  'target_component_xrefs': []})

    target_components_data = pd.DataFrame(
        data['target_components'].values.tolist())

    exposed_target_components_data = pd.DataFrame({
        #! target_component_synonyms
        'component_synonym':                      ExtractedValuesFromColumn(target_components_data, 'target_component_synonyms', 'component_synonym'),
        'syn_type':                               ExtractedValuesFromColumn(target_components_data, 'target_component_synonyms', 'syn_type'),
        #! target_component_xrefs
        'xref_id_target_component_xrefs':         ExtractedValuesFromColumn(target_components_data, 'target_component_xrefs', 'xref_id'),
        'xref_name_target_component_xrefs':       ExtractedValuesFromColumn(target_components_data, 'target_component_xrefs', 'xref_name'),
        'xref_src_db_target_component_xrefs':     ExtractedValuesFromColumn(target_components_data, 'target_component_xrefs', 'xref_src_db'),
    })

    target_components_data = target_components_data.drop(
        ['target_component_synonyms', 'target_component_xrefs'], axis=1)
    target_components_data = pd.concat(
        [target_components_data, exposed_target_components_data], axis=1)

    data = data.drop(['cross_references', 'target_components'], axis=1)
    data = pd.concat([data, exposed_data, target_components_data], axis=1)

    logger.success(
        f"Expand pandas.DataFrame() from dictionaries: SUCCESS".ljust(77))

    return data


def AddedIC50andKiToTargetsDF(data: pd.DataFrame,
                              download_activities: bool = True,
                              activities_results_folder_name: str = "results/activities",
                              print_to_console: bool = False) -> pd.DataFrame:
    """
    Добавляет в pd.DataFrame два столбца: IC50 и Ki

    Args:
        data (pd.DataFrame): исходный pd.DataFrame
        need_to_download_activities (bool, optional): нужно ли скачивать activities отдельно. Defaults to True.
        activities_results_folder_name (str, optional): название папки для скачанных activities. Defaults to "results/activities".
        print_to_console (bool, optional): нужно ли выводить логирование в консоль. Defaults to False.

    Returns:
        pd.DataFrame: расширенный pd.DataFrame
    """

    logger.info(
        f"Add 'IC50' and 'Ki' columns to pandas.DataFrame()...".ljust(77))

    try:
        data["IC50"] = data["target_chembl_id"].apply(CountActivitiesByIC50)
        data["Ki"] = data["target_chembl_id"].apply(CountActivitiesByKi)

        logger.success(
            f"Add 'IC50' and 'Ki' columns to pandas.DataFrame(): SUCCESS".ljust(77))

        if download_activities:
            DownloadChEMBLActivities(data,
                                     results_folder_name=activities_results_folder_name,
                                     print_to_console=print_to_console)

            data["IC50_new"] = data["IC50_new"].astype(int)
            data["Ki_new"] = data["Ki_new"].astype(int)

    except Exception as exception:
        logger.error(f"{exception}".ljust(77))

    return data


def DownloadTargetsFromIdList(target_chembl_id_list: list[str] = [],
                              results_folder_name: str = "results/targets",
                              primary_analysis_folder_name: str = "primary_analysis",
                              need_primary_analysis: bool = False,
                              download_activities: bool = True,
                              activities_results_folder_name: str = "results/activities",
                              print_to_console: bool = False) -> None:
    """
    Скачивает цели по списку id из базы ChEMBL, сохраняя их в .csv файл

    Args:
        target_chembl_id_list (list[str], optional): список id. Defaults to []: для скачивания всех целей.
        results_folder_name (str, optional): имя папки для закачки. Defaults to "results/targets".
        primary_analysis_folder_name (str, optional): имя папки для сохранения данных о первичном анализе. Defaults to "primary_analysis".
        need_primary_analysis (bool, optional): нужно ли проводить первичный анализ. Defaults to False.
        download_activities (bool, optional): нужно ли скачивать активности к целям по IC50 и Ki. Defaults to True.
        activities_results_folder_name (str, optional): имя папки для закачки activities. Defaults to "results/activities". 
        print_to_console (bool, optional): нужно ли выводить логирование в консоль. Defaults to False.
    """

    try:
        logger.info(
            f"Downloading targets...".ljust(77))
        targets_with_ids: QuerySet = QuerySetTargetsFromIdList(
            target_chembl_id_list)

        if target_chembl_id_list == []:
            targets_with_ids = QuerySetAllTargets()

        logger.info(f"Amount: {len(targets_with_ids)}".ljust(77))
        logger.success(f"Downloading targets: SUCCESS".ljust(77))

        try:
            logger.info(
                "Collecting targets to pandas.DataFrame()...".ljust(77))

            data_frame = AddedIC50andKiToTargetsDF(
                ExpandedFromDictionariesTargetsDF(
                    pd.DataFrame(targets_with_ids)),
                download_activities=download_activities,
                activities_results_folder_name=activities_results_folder_name,
                print_to_console=print_to_console)

            UpdateLoggerFormat("ChEMBL__targets", "fg #CBDD7C")

            logger.success(
                "Collecting targets to pandas.DataFrame(): SUCCESS".ljust(77))

            logger.info(
                f"Collecting targets to .csv file in '{results_folder_name}'...".ljust(77))

            if need_primary_analysis:
                PrimaryAnalysisByColumns(data_frame=data_frame,
                                         data_name=f"targets_data_from_ChEMBL",
                                         folder_name=f"{
                                             results_folder_name}/{primary_analysis_folder_name}",
                                         print_to_console=print_to_console)

                UpdateLoggerFormat("ChEMBL__targets", "fg #CBDD7C")

            file_name: str = f"{
                results_folder_name}/targets_data_from_ChEMBL.csv"

            data_frame.to_csv(file_name, sep=';', index=False)
            logger.success(
                f"Collecting targets to .csv file in '{results_folder_name}': SUCCESS".ljust(77))

        except Exception as exception:
            logger.error(f"{exception}".ljust(77))

    except Exception as exception:
        logger.error(f"{exception}".ljust(77))
