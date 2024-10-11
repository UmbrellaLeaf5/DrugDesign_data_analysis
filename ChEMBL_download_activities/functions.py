# type: ignore

from chembl_webresource_client.new_client import new_client
from chembl_webresource_client.query_set import QuerySet

from Utils.primary_analysis import *


def QuerySetActivitiesByIC50(target_id: str) -> QuerySet:
    return new_client.activity.filter(target_chembl_id=target_id).filter(standard_type="IC50")


def QuerySetActivitiesByKi(target_id: str) -> QuerySet:
    return new_client.activity.filter(target_chembl_id=target_id).filter(standard_type="Ki")


def CountActivitiesByIC50(target_id: str) -> int:
    return len(QuerySetActivitiesByIC50(target_id))


def CountActivitiesByKi(target_id: str) -> int:
    return len(QuerySetActivitiesByKi(target_id))


def CleanedActivitiesDF(data: pd.DataFrame) -> pd.DataFrame:
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
                          'value'], axis=1)

    except Exception as exception:
        logger.warning(f"{exception}".ljust(77))

    # TODO: произвести фильтрацию:
    # relation == '='
    # standard_units == 'nM'
    # standard_value == float
    # для повторов по molecule_chembl_id - считать медиану nM
    # поменять столбцы местами
    # ? скачать по molecule_chembl_id также информацию о targets

    return data
