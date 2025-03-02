"""
ChEMBL_download_targets/functions.py

Этот модуль содержит функции для загрузки данных о целевых белках (targets)
из базы данных ChEMBL, расширения словарей в DataFrame, добавления
информации об активностях и сохранения результатов в CSV-файл.
"""

from chembl_webresource_client.new_client import new_client
from chembl_webresource_client.query_set import QuerySet

from ChEMBL_download_activities.download import DownloadTargetChEMBLActivities
from ChEMBL_download_activities.functions import \
    CountTargetActivitiesByIC50, CountTargetActivitiesByKi

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

    # получаем все цели из базы ChEMBL.
    return new_client.target.filter()  # type: ignore


@ReTry()
def QuerySetTargetsFromIdList(target_chembl_id_list: list[str]) -> QuerySet:
    """
    Возвращает цели по списку id из базы ChEMBL.

    Args:
        target_chembl_id_list (list[str]): список id.

    Returns:
        QuerySet: набор целей по списку id.
    """

    # получаем цели по списку id из базы ChEMBL.
    return new_client.target.filter(  # type: ignore
        target_chembl_id__in=target_chembl_id_list)


def ExpandedFromDictionariesTargetsDF(data: pd.DataFrame) -> pd.DataFrame:
    """
    Избавляет pd.DataFrame от словарей и списков словарей в столбцах, разбивая
    их на подстолбцы.

    Args:
        data (pd.DataFrame): исходный pd.DataFrame.

    Returns:
        pd.DataFrame: "раскрытый" pd.DataFrame.
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

        # извлекаем значения из указанного столбца DataFrame.
        return df[column_name].apply(
            lambda x: [d[key] for d in x] if x else [])

    # извлекаем значения из столбца cross_references.
    exposed_data = pd.DataFrame({
        "xref_id": ExtractedValuesFromColumn(data, "cross_references", "xref_id"),
        "xref_name": ExtractedValuesFromColumn(data, "cross_references", "xref_name"),
        "xref_src": ExtractedValuesFromColumn(data, "cross_references", "xref_src"),
    })

    # избавляемся от списков, так как в них находятся одиночные словари.
    data["target_components"] = data["target_components"].apply(
        lambda x: x[0] if x else {"accession": None,
                                  "component_description": None,
                                  "component_id": None,
                                  "component_type": None,
                                  "relationship": None,
                                  "target_component_synonyms": [],
                                  "target_component_xrefs": []})

    # создаем DataFrame из столбца target_components.
    target_components_data = pd.DataFrame(
        data["target_components"].values.tolist())

    # извлекаем значения из столбца target_component_synonyms и
    # target_component_xrefs.
    exposed_target_components_data = pd.DataFrame({
        # ! target_component_synonyms
        "component_synonym": ExtractedValuesFromColumn(
            target_components_data, "target_component_synonyms", "component_synonym"),
        "syn_type": ExtractedValuesFromColumn(
            target_components_data, "target_component_synonyms", "syn_type"),

        # ! target_component_xrefs
        "xref_id_target_component_xrefs": ExtractedValuesFromColumn(
            target_components_data, "target_component_xrefs", "xref_id"),
        "xref_name_target_component_xrefs": ExtractedValuesFromColumn(
            target_components_data, "target_component_xrefs", "xref_name"),
        "xref_src_db_target_component_xrefs": ExtractedValuesFromColumn(
            target_components_data, "target_component_xrefs", "xref_src_db"),
    })

    # удаляем столбцы target_component_synonyms и target_component_xrefs.
    target_components_data = target_components_data.drop(
        ["target_component_synonyms", "target_component_xrefs"], axis=1)
    # объединяем DataFrames.
    target_components_data = pd.concat(
        [target_components_data, exposed_target_components_data], axis=1)

    # удаляем столбцы cross_references и target_components.
    data = data.drop(["cross_references", "target_components"], axis=1)
    # объединяем DataFrames.
    data = pd.concat([data, exposed_data, target_components_data], axis=1)

    return data


@ReTry(attempts_amount=1)
def AddedIC50andKiToTargetsDF(data: pd.DataFrame) -> pd.DataFrame:
    """
    Добавляет столбцы 'IC50' и 'Ki' в DataFrame с данными о целевых белках
    (targets), подсчитывая количество соответствующих активностей из базы данных
    ChEMBL, а также опционально скачивает новые активности.

    Args:
        data (pd.DataFrame): DataFrame с данными о целевых белках.

    Returns:
        pd.DataFrame: DataFrame с добавленными столбцами 'IC50' и 'Ki',
                      содержащими количество соответствующих активностей.
    """

    # получаем конфигурацию для скачивания целей.
    targets_config: Config = config["ChEMBL_download_targets"]

    v_logger.info(
        "Adding 'IC50' and 'Ki' columns to pandas.DataFrame...",
        LogMode.VERBOSELY)

    # добавляем столбец 'IC50', подсчитывая активности.
    data["IC50"] = data["target_chembl_id"].apply(
        CountTargetActivitiesByIC50)
    # добавляем столбец 'Ki', подсчитывая активности.
    data["Ki"] = data["target_chembl_id"].apply(
        CountTargetActivitiesByKi)

    v_logger.success(
        "Adding 'IC50' and 'Ki' columns to pandas.DataFrame!",
        LogMode.VERBOSELY)

    # если нужно скачивать активности.
    if targets_config["download_activities"]:
        # скачиваем активности для целевых белков.
        DownloadTargetChEMBLActivities(data)

        try:
            # оставляем только строки, в которых есть IC50_new и Ki_new
            data = data[(data['IC50_new'].notna()
                         ) & (
                data['Ki_new'].notna())]

            # преобразуем типы столбцов.
            data["IC50_new"] = data["IC50_new"].astype(int)
            data["Ki_new"] = data["Ki_new"].astype(int)

        # это исключение может возникнуть, если колонки нет.
        except KeyError as exception:
            # новых activities не скачалось, т.е. значение пересчитывать не надо.
            if not config["skip_downloaded"]:
                raise exception

        # это исключение может возникнуть, если какое-то значение оказалось невалидным.
        except pd.errors.IntCastingNaNError:
            v_logger.warning("Cannot convert non-finite values!")

    return data


@ReTry(attempts_amount=1)
def DownloadTargetsFromIdList():
    """
    Скачивает данные о целевых белках (targets) из ChEMBL по списку
    идентификаторов, добавляет информацию об активностях IC50 и Ki, проводит
    первичный анализ и сохраняет результаты в CSV-файл.
    """

    # получаем конфигурацию для скачивания целей.
    targets_config: Config = config["ChEMBL_download_targets"]

    v_logger.info("Downloading targets...", LogMode.VERBOSELY)

    # получаем цели по списку id.
    targets_with_ids: QuerySet = QuerySetTargetsFromIdList(
        targets_config["id_list"])

    # если список id пуст, получаем все цели.
    if targets_config["id_list"] == []:
        targets_with_ids = QuerySetAllTargets()

    v_logger.info(f"Amount: {len(targets_with_ids)}")  # type: ignore
    v_logger.success("Downloading targets!", LogMode.VERBOSELY)
    v_logger.info("Collecting targets to pandas.DataFrame..",
                  LogMode.VERBOSELY)

    # добавляем информацию об активностях IC50 и Ki.
    data_frame = AddedIC50andKiToTargetsDF(
        ExpandedFromDictionariesTargetsDF(
            pd.DataFrame(targets_with_ids)  # type: ignore
        ))

    v_logger.UpdateFormat(targets_config["logger_label"],
                          targets_config["logger_color"])

    v_logger.success("Collecting targets to pandas.DataFrame!",
                     LogMode.VERBOSELY)
    v_logger.info(
        f"Collecting targets to .csv file in "
        f"'{targets_config["results_folder_name"]}'...",
        LogMode.VERBOSELY)

    # формируем имя файла.
    file_name: str = f"{targets_config["results_folder_name"]}/"\
        f"{targets_config["results_file_name"]}.csv"

    # сохраняем DataFrame в CSV-файл.
    data_frame.to_csv(file_name, sep=";", index=False)

    v_logger.success(
        f"Collecting targets to .csv file in "
        f"'{targets_config["results_folder_name"]}'!",
        LogMode.VERBOSELY)
