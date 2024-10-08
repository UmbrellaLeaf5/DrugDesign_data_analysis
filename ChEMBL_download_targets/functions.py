# type: ignore

from chembl_webresource_client.new_client import new_client
from chembl_webresource_client.query_set import QuerySet

from Utils.logger_funcs import *
from Utils.primary_analysis import *


def QuerySetAllTargets() -> QuerySet:
    """
    Возвращает все цели из базы ChEMBL

    Returns:
        QuerySet: набор всех целей
    """

    return new_client.target.filter()


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


def AddedIC50andKiToTargetsDF(data: pd.DataFrame) -> pd.DataFrame:
    """
    Добавляет в pd.DataFrame два столбца: IC50 и Ki

    Args:
        data (pd.DataFrame): исходный pd.DataFrame

    Returns:
        pd.DataFrame: расширенный pd.DataFrame
    """

    logger.info(
        f"Add 'IC50' and 'Ki' columns to pandas.DataFrame()...".ljust(77))

    def CountIC50(target_id: str) -> int:
        return len(new_client.activity.filter(target_chembl_id=target_id).filter(standard_type="IC50"))

    def CountKi(target_id: str) -> int:
        return len(new_client.activity.filter(target_chembl_id=target_id).filter(standard_type="Ki"))

    data["IC50"] = data["target_chembl_id"].apply(CountIC50)
    data["Ki"] = data["target_chembl_id"].apply(CountKi)

    logger.success(
        f"Add 'IC50' and 'Ki' columns to pandas.DataFrame(): SUCCESS".ljust(77))

    return data


def DownloadTargetsFromIdList(target_chembl_id_list: list[str],
                              results_folder_name: str = "targets_results",
                              primary_analysis_folder_name: str = "primary_analysis",
                              need_primary_analysis: bool = False) -> None:
    """
    Скачивает цели по списку id из базы ChEMBL, сохраняя их в .csv файл

    Args:
        target_chembl_id_list (list[str]): список id
        results_folder_name (str, optional): имя папки для закачки. Defaults to "targets_results".
        primary_analysis_folder_name (str, optional): имя папки для сохранения данных о первичном анализе. Defaults to "primary_analysis".
        need_primary_analysis (bool, optional): нужно ли проводить первичный анализ. Defaults to False.
    """

    try:
        logger.info(
            f"Downloading targets...".ljust(77))
        targets_with_ids: QuerySet = QuerySetTargetsFromIdList(
            target_chembl_id_list)

        logger.info(
            ("Amount:" + f"{len(targets_with_ids)}").ljust(77))
        logger.success(
            f"Downloading targets: SUCCESS".ljust(77))

        try:
            logger.info(
                "Collecting targets to pandas.DataFrame()...".ljust(77))
            data_frame = AddedIC50andKiToTargetsDF(
                ExpandedFromDictionariesTargetsDF(pd.DataFrame(targets_with_ids)))
            logger.success(
                "Collecting targets to pandas.DataFrame(): SUCCESS".ljust(77))

            logger.info(
                f"Collecting targets to .csv file in '{results_folder_name}'...".ljust(77))

            if need_primary_analysis:
                DataAnalysisByColumns(data_frame,
                                      f"targets_data_from_ChEMBL",
                                      f"{results_folder_name}/{primary_analysis_folder_name}")

                LoggerFormatUpdate("ChEMBL__targets", "yellow")

            file_name: str = f"{
                results_folder_name}/targets_data_from_ChEMBL.csv"

            data_frame.to_csv(file_name, index=False)
            logger.success(
                f"Collecting targets to .csv file in '{results_folder_name}': SUCCESS".ljust(77))

        except Exception as exception:
            logger.error(f"{exception}".ljust(77))

    except Exception as exception:
        logger.error(f"{exception}".ljust(77))


def DownloadAllTargets(results_folder_name: str = "targets_results",
                       primary_analysis_folder_name: str = "primary_analysis",
                       need_primary_analysis: bool = False) -> None:
    """
    Скачивает все цели из базы ChEMBL

    Args:
        results_folder_name (str, optional): имя папки для закачки. Defaults to "targets_results".
        primary_analysis_folder_name (str, optional): имя папки для сохранения данных о первичном анализе. Defaults to "primary_analysis".
        need_primary_analysis (bool, optional): нужно ли проводить первичный анализ. Defaults to False.
    """

    try:
        logger.info(
            f"Downloading targets...".ljust(77))
        targets: QuerySet = QuerySetAllTargets()

        logger.info(
            ("Amount:" + f"{len(targets)}").ljust(77))
        logger.success(
            f"Downloading targets: SUCCESS".ljust(77))

        try:
            logger.info(
                "Collecting targets to pandas.DataFrame()...".ljust(77))
            data_frame = ExpandedFromDictionariesTargetsDF(pd.DataFrame(
                targets))
            logger.success(
                "Collecting targets to pandas.DataFrame(): SUCCESS".ljust(77))

            logger.info(
                f"Collecting targets to .csv file in '{results_folder_name}'...".ljust(77))

            if need_primary_analysis:
                DataAnalysisByColumns(data_frame,
                                      f"targets_data_from_ChEMBL",
                                      f"{results_folder_name}/{primary_analysis_folder_name}")

                LoggerFormatUpdate("ChEMBL__targets", "yellow")

            file_name: str = f"{
                results_folder_name}/targets_data_from_ChEMBL.csv"

            data_frame.to_csv(file_name, index=False)
            logger.success(
                f"Collecting targets to .csv file in '{results_folder_name}': SUCCESS".ljust(77))

        except Exception as exception:
            logger.error(f"{exception}".ljust(77))

    except Exception as exception:
        logger.error(f"{exception}".ljust(77))
