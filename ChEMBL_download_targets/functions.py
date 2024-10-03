# type: ignore

from chembl_webresource_client.new_client import new_client
from chembl_webresource_client.query_set import QuerySet
import pandas as pd
import os


def QuerySetAllTargets() -> QuerySet:
    """      
    QuerySetAllTargets

    Returns:
        QuerySet: список всех целей
    """

    return new_client.target.filter()


def QuerySetTargetsFromIdList(target_chembl_id_list: list[str]) -> QuerySet:
    """
    QuerySetTargetsFromIdList - функция, которая возвращает QuerySet по целям 
    с target_chembl_id из списка

    Args:
        target_chembl_id_list (list[str]): список id

    Returns:
        QuerySet: список целей
    """
    return new_client.target.filter(target_chembl_id__in=target_chembl_id_list)


def ExpandedFromDictionaryColumnsDFTargets(data: pd.DataFrame) -> pd.DataFrame:
    """
    ExpandedFromDictionaryColumnsDFTargets - функция, которая переписывает словари и списки 
    словарей в таблице в отдельные столбцы

    Args:
        data (pd.DataFrame): изначальная таблица

    Returns:
        pd.DataFrame: "раскрытая" таблица
    """

    def ExtractedValuesFromColumn(df: pd.DataFrame, column_name: str, key: str) -> pd.Series:
        return df[column_name].apply(lambda x: [d[key] for d in x] if x else [])

    exposed_data = pd.DataFrame({
        #! cross_references
        'xref_id':                                ExtractedValuesFromColumn(data, 'cross_references', 'xref_id'),
        'xref_name':                              ExtractedValuesFromColumn(data, 'cross_references', 'xref_name'),
        'xref_src':                               ExtractedValuesFromColumn(data, 'cross_references', 'xref_src'),
    })

    # избавлюсь от списков, так как в них находятся одиночные словари
    data['target_components'] = data['target_components'].apply(lambda x: x[0])

    target_components_data = pd.DataFrame(
        data['target_components'].values.tolist())

    exposed_narrowed_data = pd.DataFrame({
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
        [target_components_data, exposed_narrowed_data], axis=1)

    data = data.drop(['cross_references', 'target_components'], axis=1)
    return pd.concat([data, exposed_data, target_components_data], axis=1)


def DeleteFilesInFolder(folder_path: str, except_files: list[str]) -> None:
    """
    Удаляет все файлы в указанной папке, кроме файлов в списке исключений.

    Args:
        folder_path (str): путь к папке.
        except_files (list[str]): список имен файлов, которые нужно исключить из удаления.
    """

    for file_name in os.listdir(folder_path):
        full_file_path = os.path.join(folder_path, file_name)

        if os.path.isfile(full_file_path) and file_name not in except_files:
            os.remove(full_file_path)


def IsFileInFolder(folder_path: str, file_name: str) -> bool:
    """
    Проверяет, существует ли файл в указанной папке.

    Args:
      file_name: путь к файлу, который нужно проверить.
      folder_path: путь к папке, в которой нужно проверить наличие файла.

    Returns:
      True, если файл существует в папке, в противном случае False.
    """

    full_file_path = os.path.join(folder_path, file_name)
    return os.path.exists(full_file_path)
