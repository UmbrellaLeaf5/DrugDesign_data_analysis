# type: ignore

from chembl_webresource_client.new_client import new_client
from chembl_webresource_client.query_set import QuerySet

from Utils.decorators import Retry
from Utils.file_and_logger_funcs import *
from Utils.list_and_dataframe_funcs import *


@Retry()
def QuerySetActivitiesByIC50(target_id: str) -> QuerySet:
    """
    Возвращает активности по target_id по IC50

    Args:
        target_id (str): идентификатор цели из базы ChEMBL

    Returns:
        QuerySet: набор активностей
    """

    return new_client.activity.filter(target_chembl_id=target_id).filter(standard_type="IC50")


@Retry()
def QuerySetActivitiesByKi(target_id: str) -> QuerySet:
    """
    Возвращает активности по target_id по Ki

    Args:
        target_id (str): идентификатор цели из базы ChEMBL

    Returns:
        QuerySet: набор активностей
    """

    return new_client.activity.filter(target_chembl_id=target_id).filter(standard_type="Ki")


def CountActivitiesByIC50(target_id: str) -> int:
    """
    Подсчитывает кол-во активностей по target_id по IC50
    (иначе говоря, численное значение IC50 для конкретной цели)

    Args:
        target_id (str): идентификатор цели из базы ChEMBL

    Returns:
        int: количество
    """

    return len(QuerySetActivitiesByIC50(target_id))


def CountActivitiesByKi(target_id: str) -> int:
    """
    Подсчитывает кол-во активностей по target_id по Ki
    (иначе говоря, численное значение Ki для конкретной цели)

    Args:
        target_id (str): идентификатор цели из базы ChEMBL

    Returns:
        int: количество
    """

    return len(QuerySetActivitiesByKi(target_id))


def CleanedActivitiesDF(data: pd.DataFrame, target_id: str, activities_type: str,
                        print_to_console: bool = False) -> pd.DataFrame:
    """
    Производит чистку выборки activities конкретной цели по IC50 и Ki

    Args:
        data (pd.DataFrame): выборка activities
        target_id (str): идентификатор цели
        activities_type (str): IC50 или Ki
        print_to_console (bool, optional): нужно ли выводить логирование в консоль. Defaults to False.

    Returns:
        pd.DataFrame: очищенная выборка
    """

    if print_to_console:
        logger.info(f"Start cleaning {activities_type} activities DataFrame from {
            target_id}...".ljust(77))

        logger.info(f"Deleting useless columns...".ljust(77))

    try:
        data = data.drop(['activity_id', 'activity_properties',
                          'document_journal', 'document_year',
                          'molecule_pref_name', 'pchembl_value',
                          'potential_duplicate', 'qudt_units',
                          'record_id', 'src_id', 'standard_flag',
                          'standard_text_value', 'standard_upper_value',
                          'target_chembl_id', 'target_pref_name',
                          'target_tax_id', 'text_value', 'toid',
                          'type', 'units', 'uo_units', 'upper_value',
                          'value', 'ligand_efficiency'], axis=1)

        if print_to_console:
            logger.success(f"Deleting useless columns: SUCCESS".ljust(77))

            logger.info(f"Deleting inappropriate elements...".ljust(77))

        data = data[data['relation'] == '=']
        data = data[data['standard_units'] == 'nM']
        data = data[data['target_organism'] == "Homo sapiens"]
        data = data[data['standard_type'].isin(['IC50', 'Ki'])]

        data['standard_value'] = data['standard_value'].astype(float)
        data = data[data['standard_value'] <= 1000000000]

        data['activity_comment'] = data['activity_comment'].replace(
            "Not Determined", None)

        data = data.drop(['target_organism', 'standard_type'], axis=1)

        if print_to_console:
            logger.success(
                f"Deleting inappropriate elements: SUCCESS".ljust(77))

            logger.info(
                f"Calculating median for 'standard value'...".ljust(77))

        data = MedianDedupedDF(data, "molecule_chembl_id", "standard_value")

        if print_to_console:
            logger.success(
                f"Calculating median for 'standard value': SUCCESS".ljust(77))

            logger.info(
                f"Reindexing columns in logical order...".ljust(77))

        data = data.reindex(columns=["molecule_chembl_id", "parent_molecule_chembl_id",
                                     "canonical_smiles", "document_chembl_id", "standard_relation",
                                     "relation", "standard_value", "standard_units", 'assay_chembl_id',
                                     'assay_description', 'assay_type', 'assay_variant_accession',
                                     'assay_variant_mutation', 'action_type', 'activity_comment',
                                     'data_validity_comment', 'data_validity_description',
                                     'bao_endpoint', 'bao_format', 'bao_label'])

        if print_to_console:
            logger.success(
                f"Reindexing columns in logical order: SUCCESS".ljust(77))

            logger.success(f"End cleaning activities DataFrame from {
                target_id}".ljust(77))

    except Exception as exception:
        logger.error(f"{exception}".ljust(77))

    if print_to_console:
        logger.info(f"{'-' * 77}")

    return data
