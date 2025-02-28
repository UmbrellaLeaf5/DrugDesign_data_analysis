from chembl_webresource_client.new_client import new_client
from chembl_webresource_client.query_set import QuerySet

from ChEMBL_download_activities.download import DownloadTargetChEMBLActivities
from ChEMBL_download_activities.functions import CountTargetActivitiesByIC50, \
    CountTargetActivitiesByKi

from Utils.decorators import ReTry
from Utils.files_funcs import pd
from Utils.verbose_logger import v_logger, LogMode

from Configurations.config import config, Config


@ReTry()
def QuerySetAllTargets() -> QuerySet:
    """
    Возвращает все цели из базы ChEMBL.

    Returns:
        QuerySet: набор всех целей
    """

    return new_client.target.filter()  # type: ignore


@ReTry()
def QuerySetTargetsFromIdList(target_chembl_id_list: list[str]) -> QuerySet:
    """
    Возвращает цели по списку id из базы ChEMBL.

    Args:
        target_chembl_id_list (list[str]): список id

    Returns:
        QuerySet: набор целей по списку id
    """

    return new_client.target.filter(target_chembl_id__in=target_chembl_id_list)  # type: ignore


def ExpandedFromDictionariesTargetsDF(data: pd.DataFrame) -> pd.DataFrame:
    """
    Избавляет pd.DataFrame от словарей и списков словарей в столбцах, 
    разбивая их на подстолбцы.

    Args:
        data (pd.DataFrame): исходный pd.DataFrame

    Returns:
        pd.DataFrame: "раскрытый" pd.DataFrame
    """

    def ExtractedValuesFromColumn(df: pd.DataFrame,
                                  column_name: str,
                                  key: str) -> pd.Series:
        """
        Извлекает значения из указанного столбца DataFrame, который содержит 
        списки словарей, на основе заданного ключа.

        Args:
            df (pd.DataFrame): DataFrame, из которого нужно извлечь значения.
            column_name (str): название столбца, содержащего списки словарей.
            key (str): ключ, по которому нужно извлечь значения из словарей.

        Returns:
            pd.Series: Series, содержащий списки извлеченных значений.
        """

        return df[column_name].apply(lambda x: [d[key] for d in x] if x else [])

    exposed_data = pd.DataFrame({
        #! cross_references
        "xref_id":              ExtractedValuesFromColumn(data, "cross_references",
                                                          "xref_id"),
        "xref_name":            ExtractedValuesFromColumn(data, "cross_references",
                                                          "xref_name"),
        "xref_src":             ExtractedValuesFromColumn(data, "cross_references",
                                                          "xref_src"),
    })

    # избавлюсь от списков, так как в них находятся одиночные словари
    data["target_components"] = data["target_components"].apply(
        lambda x: x[0] if x else {"accession": None,
                                  "component_description": None,
                                  "component_id": None,
                                  "component_type": None,
                                  "relationship": None,
                                  "target_component_synonyms": [],
                                  "target_component_xrefs": []})

    target_components_data = pd.DataFrame(
        data["target_components"].values.tolist())

    exposed_target_components_data = pd.DataFrame({
        #! target_component_synonyms
        "component_synonym":                      ExtractedValuesFromColumn(
            target_components_data, "target_component_synonyms", "component_synonym"),
        "syn_type":                               ExtractedValuesFromColumn(
            target_components_data, "target_component_synonyms", "syn_type"),
        #! target_component_xrefs
        "xref_id_target_component_xrefs":         ExtractedValuesFromColumn(
            target_components_data, "target_component_xrefs", "xref_id"),
        "xref_name_target_component_xrefs":       ExtractedValuesFromColumn(
            target_components_data, "target_component_xrefs", "xref_name"),
        "xref_src_db_target_component_xrefs":     ExtractedValuesFromColumn(
            target_components_data, "target_component_xrefs", "xref_src_db"),
    })

    target_components_data = target_components_data.drop(
        ["target_component_synonyms", "target_component_xrefs"], axis=1)
    target_components_data = pd.concat(
        [target_components_data, exposed_target_components_data], axis=1)

    data = data.drop(["cross_references", "target_components"], axis=1)
    data = pd.concat([data, exposed_data, target_components_data], axis=1)

    return data


@ReTry(attempts_amount=1)
def AddedIC50andKiToTargetsDF(data: pd.DataFrame
                              ) -> pd.DataFrame:
    """
    Добавляет столбцы 'IC50' и 'Ki' в DataFrame с данными о целевых белках (targets),
    подсчитывая количество соответствующих активностей из базы данных ChEMBL,
    а также опционально скачивает новые активности.

    Args:
        data (pd.DataFrame): DataFrame с данными о целевых белках.

    Returns:
        pd.DataFrame: DataFrame с добавленными столбцами 'IC50' и 'Ki',
                      содержащими количество соответствующих активностей.
    """

    targets_config: Config = config["ChEMBL_download_targets"]

    v_logger.info(
        f"Adding 'IC50' and 'Ki' columns to pandas.DataFrame...",
        LogMode.VERBOSELY)

    data["IC50"] = data["target_chembl_id"].apply(
        CountTargetActivitiesByIC50)
    data["Ki"] = data["target_chembl_id"].apply(
        CountTargetActivitiesByKi)

    v_logger.success(
        f"Adding 'IC50' and 'Ki' columns to pandas.DataFrame!",
        LogMode.VERBOSELY)

    if targets_config["download_activities"]:
        DownloadTargetChEMBLActivities(data)

        try:
            data["IC50_new"] = data["IC50_new"].astype(int)
            data["Ki_new"] = data["Ki_new"].astype(int)

        except KeyError as exception:  # это исключение может возникнуть, если колонки нет
            # новых activities не скачалось, т.е. значение пересчитывать не надо
            if not config["skip_downloaded"]:
                raise exception

    return data


@ReTry(attempts_amount=1)
def DownloadTargetsFromIdList():
    """
    Скачивает данные о целевых белках (targets) из ChEMBL по списку идентификаторов,
    добавляет информацию об активностях IC50 и Ki, проводит первичный анализ
    и сохраняет результаты в CSV-файл.
    """

    targets_config: Config = config["ChEMBL_download_targets"]

    v_logger.info("Downloading targets...", LogMode.VERBOSELY)

    targets_with_ids: QuerySet = QuerySetTargetsFromIdList(targets_config["id_list"])

    if targets_config["id_list"] == []:
        targets_with_ids = QuerySetAllTargets()

    v_logger.info(f"Amount: {len(targets_with_ids)}")  # type: ignore
    v_logger.success("Downloading targets!", LogMode.VERBOSELY)
    v_logger.info("Collecting targets to pandas.DataFrame..", LogMode.VERBOSELY)

    data_frame = AddedIC50andKiToTargetsDF(
        ExpandedFromDictionariesTargetsDF(
            pd.DataFrame(targets_with_ids)  # type: ignore
        ))

    v_logger.UpdateFormat(targets_config["logger_label"],
                          targets_config["logger_color"])

    v_logger.success("Collecting targets to pandas.DataFrame!", LogMode.VERBOSELY)
    v_logger.info(
        f"Collecting targets to .csv file in "
        f"'{targets_config["results_folder_name"]}'...", LogMode.VERBOSELY)

    file_name: str = f"{targets_config["results_folder_name"]}/{targets_config["results_file_name"]}.csv"

    data_frame.to_csv(file_name, sep=";", index=False)

    v_logger.success(
        f"Collecting targets to .csv file in "
        f"'{targets_config["results_folder_name"]}'!",
        LogMode.VERBOSELY)
