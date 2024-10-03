from chembl_webresource_client.new_client import new_client
from chembl_webresource_client.query_set import QuerySet
import pandas as pd
import os


def QuerySetMWRangeFilterCompounds(less_limit: int = 0, greater_limit: int = 12_546_42) -> QuerySet:
    """      
    QuerySetMolecularWeightRangeFilter - функция, которая выполняет фильтрацию по базе ChEMBL
    по диапазону ( [): полуинтервалу) молекулярного веса

    Args:
        less_limit (int, optional): нижняя граница. Defaults to 0
        greater_limit (int, optional): верхняя граница. Defaults to 12_546_42

    Raises:
        ValueError: если границы меньше являются отриц. числами
        ValueError: если, верхняя граница больше нижней

    Returns:
        QuerySet: список объектов заданной модели
    """

    if greater_limit < 0 or less_limit < 0:
        raise ValueError(
            "QuerySetMWRangeFilter: limits should be greater zero")

    if greater_limit < less_limit:
        raise ValueError(
            "QuerySetMWRangeFilter: greater_limit should be greater than less_limit")

    return new_client.molecule.filter(molecule_properties__mw_freebase__lt=greater_limit,  # type: ignore
                                      molecule_properties__mw_freebase__gte=less_limit)


def ExpandedFromDictionaryColumnsDFCompounds(data: pd.DataFrame) -> pd.DataFrame:
    """
    ExpandedFromDictionaryColumnsDataFrame - функция, которая переписывает словари и списки словарей 
    в таблице в отдельные столбцы

    Args:
        data (pd.DataFrame): изначальная таблица

    Returns:
        pd.DataFrame: "раскрытая" таблица
    """

    # cSpell:disable

    exposed_data = pd.DataFrame({
        #! cross_references
        'xref_id':                     data['cross_references'].apply(lambda x: [d['xref_id'] for d in x] if x else []),
        'xref_name':                   data['cross_references'].apply(lambda x: [d['xref_name'] for d in x] if x else []),
        'xref_src':                    data['cross_references'].apply(lambda x: [d['xref_src'] for d in x] if x else []),
        #! molecule_hierarchy
        'active_chembl_id':            [item['active_chembl_id'] if isinstance(item, dict) else None for item in data['molecule_hierarchy']],
        'molecule_chembl_id':          [item['molecule_chembl_id'] if isinstance(item, dict) else None for item in data['molecule_hierarchy']],
        'parent_chembl_id':            [item['parent_chembl_id'] if isinstance(item, dict) else None for item in data['molecule_hierarchy']],
        #! molecule_properties
        'alogp':                       [item['alogp'] if isinstance(item, dict) else None for item in data['molecule_properties']],
        'aromatic_rings':              [item['aromatic_rings'] if isinstance(item, dict) else None for item in data['molecule_properties']],
        'cx_logd':                     [item['cx_logd'] if isinstance(item, dict) else None for item in data['molecule_properties']],
        'cx_logp':                     [item['cx_logp'] if isinstance(item, dict) else None for item in data['molecule_properties']],
        'cx_most_apka':                [item['cx_most_apka'] if isinstance(item, dict) else None for item in data['molecule_properties']],
        'cx_most_bpka':                [item['cx_most_bpka'] if isinstance(item, dict) else None for item in data['molecule_properties']],
        'full_molformula':             [item['full_molformula'] if isinstance(item, dict) else None for item in data['molecule_properties']],
        'full_mwt':                    [item['full_mwt'] if isinstance(item, dict) else None for item in data['molecule_properties']],
        'hba':                         [item['hba'] if isinstance(item, dict) else None for item in data['molecule_properties']],
        'hba_lipinski':                [item['hba_lipinski'] if isinstance(item, dict) else None for item in data['molecule_properties']],
        'hbd':                         [item['hbd'] if isinstance(item, dict) else None for item in data['molecule_properties']],
        'hbd_lipinski':                [item['hbd_lipinski'] if isinstance(item, dict) else None for item in data['molecule_properties']],
        'heavy_atoms':                 [item['heavy_atoms'] if isinstance(item, dict) else None for item in data['molecule_properties']],
        'molecular_species':           [item['molecular_species'] if isinstance(item, dict) else None for item in data['molecule_properties']],
        'mw_freebase':                 [item['mw_freebase'] if isinstance(item, dict) else None for item in data['molecule_properties']],
        'mw_monoisotopic':             [item['mw_monoisotopic'] if isinstance(item, dict) else None for item in data['molecule_properties']],
        'np_likeness_score':           [item['np_likeness_score'] if isinstance(item, dict) else None for item in data['molecule_properties']],
        'num_lipinski_ro5_violations': [item['num_lipinski_ro5_violations'] if isinstance(item, dict) else None for item in data['molecule_properties']],
        'num_ro5_violations':          [item['num_ro5_violations'] if isinstance(item, dict) else None for item in data['molecule_properties']],
        'psa':                         [item['psa'] if isinstance(item, dict) else None for item in data['molecule_properties']],
        'qed_weighted':                [item['qed_weighted'] if isinstance(item, dict) else None for item in data['molecule_properties']],
        'ro3_pass':                    [item['ro3_pass'] if isinstance(item, dict) else None for item in data['molecule_properties']],
        'rtb':                         [item['rtb'] if isinstance(item, dict) else None for item in data['molecule_properties']],
        #! molecule_structures
        'canonical_smiles':            [item['canonical_smiles'] if isinstance(item, dict) else None for item in data['molecule_structures']],
        # 'molfile':                   [item['molfile'] if isinstance(item, dict) else None for item in data['molecule_structures']], - какая-то стрёмная хрень с RDKit
        'standard_inchi':              [item['standard_inchi'] if isinstance(item, dict) else None for item in data['molecule_structures']],
        'standard_inchi_key':          [item['standard_inchi_key'] if isinstance(item, dict) else None for item in data['molecule_structures']],
        #! molecule_synonyms
        'molecule_synonym':            data['molecule_synonyms'].apply(lambda x: [d['molecule_synonym'] for d in x] if x else []),
        'syn_type':                    data['molecule_synonyms'].apply(lambda x: [d['syn_type'] for d in x] if x else []),
        'synonyms':                    data['molecule_synonyms'].apply(lambda x: [d['synonyms'] for d in x] if x else []),
    })

    data = data.drop(['cross_references', 'molecule_hierarchy',
                     'molecule_properties', 'molecule_structures', 'molecule_synonyms'], axis=1)

    # cSpell:enable

    return pd.concat([data, exposed_data], axis=1)


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
