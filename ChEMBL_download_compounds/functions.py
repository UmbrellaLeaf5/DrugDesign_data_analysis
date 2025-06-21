"""
ChEMBL_download_compounds/functions.py

Этот модуль содержит функции для запроса соединений из базы данных ChEMBL по
диапазону молекулярной массы, преобразования данных в DataFrame и сохранения
molfiles в SDF формат.
"""

from chembl_webresource_client.new_client import new_client
from chembl_webresource_client.query_set import QuerySet

from Utils.decorators import ReTry
from Utils.files_funcs import SaveMolfilesToSDF, pd
from Utils.verbose_logger import LogMode, v_logger


@ReTry()
def QuerySetCompoundsByMWRange(less_limit: int, greater_limit: int) -> QuerySet:
  """
  Возвращает молекулы в диапазоне молекулярной массы
  [less_limit; greater_limit) из базы ChEMBL.

  Args:
      less_limit (int): нижняя граница.
      greater_limit (int): верхняя граница.

  Raises:
      ValueError: границы должны быть больше нуля.
      ValueError: greater_limit должен быть больше less_limit.

  Returns:
      QuerySet: набор молекул в диапазоне.
  """

  # проверяем, что границы больше нуля.
  if greater_limit < 0 or less_limit < 0:
    raise ValueError("QuerySetMWRangeFilter: limits should be greater zero")

  # проверяем, что верхняя граница больше нижней.
  if greater_limit < less_limit:
    raise ValueError(
      "QuerySetMWRangeFilter: greater_limit should be greater than less_limit"
    )

  # фильтруем молекулы по диапазону молекулярной массы.
  return new_client.molecule.filter(  # type: ignore
    molecule_properties__mw_freebase__lt=greater_limit,
    molecule_properties__mw_freebase__gte=less_limit,
  )


def ExpandedFromDictionariesCompoundsDF(data: pd.DataFrame) -> pd.DataFrame:
  """
  Избавляет pd.DataFrame от словарей и списков словарей в столбцах, разбивая
  их на подстолбцы.

  Args:
      data (pd.DataFrame): исходный pd.DataFrame.

  Returns:
      pd.DataFrame: "раскрытый" pd.DataFrame.
  """

  def ExtractedValuesFromColumn(
    df: pd.DataFrame, column_name: str, key: str, is_list: bool = True
  ) -> pd.Series:
    """
    Извлекает значения из указанного столбца DataFrame на основе заданного
    ключа.

    Args:
        df (pd.DataFrame): DataFrame, из которого нужно извлечь значения.
        column_name (str): название столбца, из которого нужно извлечь
                             значения.
        key (str): ключ, по которому нужно извлечь значения из словарей.
        is_list (bool, optional): флаг, указывающий, является ли значение
                                   в столбце списком словарей.

    Returns:
        pd.Series: Series, содержащий извлеченные значения.
    """

    # если значение в столбце - список словарей.
    if is_list:
      return df[column_name].apply(lambda x: [d[key] for d in x] if x else [])

    # если значение в столбце - не список словарей.
    return [item[key] if isinstance(item, dict) else None for item in df[column_name]]  # type: ignore

  # извлекаем значения из различных столбцов DataFrame.
  exposed_data = pd.DataFrame(
    {
      # ! cross_references
      "xref_id": ExtractedValuesFromColumn(data, "cross_references", "xref_id"),
      "xref_name": ExtractedValuesFromColumn(data, "cross_references", "xref_name"),
      "xref_src": ExtractedValuesFromColumn(data, "cross_references", "xref_src"),
      # ! molecule_hierarchy
      "active_chembl_id": ExtractedValuesFromColumn(
        data, "molecule_hierarchy", "active_chembl_id", is_list=False
      ),
      "molecule_chembl_id": ExtractedValuesFromColumn(
        data, "molecule_hierarchy", "molecule_chembl_id", is_list=False
      ),
      "parent_chembl_id": ExtractedValuesFromColumn(
        data, "molecule_hierarchy", "parent_chembl_id", is_list=False
      ),
      # ! molecule_properties
      "alogp": ExtractedValuesFromColumn(
        data, "molecule_properties", "alogp", is_list=False
      ),
      "aromatic_rings": ExtractedValuesFromColumn(
        data, "molecule_properties", "aromatic_rings", is_list=False
      ),
      "cx_logd": ExtractedValuesFromColumn(
        data, "molecule_properties", "cx_logd", is_list=False
      ),
      "cx_logp": ExtractedValuesFromColumn(
        data, "molecule_properties", "cx_logp", is_list=False
      ),
      "cx_most_apka": ExtractedValuesFromColumn(
        data, "molecule_properties", "cx_most_apka", is_list=False
      ),
      "cx_most_bpka": ExtractedValuesFromColumn(
        data, "molecule_properties", "cx_most_bpka", is_list=False
      ),
      "full_molformula": ExtractedValuesFromColumn(
        data, "molecule_properties", "full_molformula", is_list=False
      ),
      "full_mwt": ExtractedValuesFromColumn(
        data, "molecule_properties", "full_mwt", is_list=False
      ),
      "hba": ExtractedValuesFromColumn(data, "molecule_properties", "hba", is_list=False),
      "hba_lipinski": ExtractedValuesFromColumn(
        data, "molecule_properties", "hba_lipinski", is_list=False
      ),
      "hbd": ExtractedValuesFromColumn(data, "molecule_properties", "hbd", is_list=False),
      "hbd_lipinski": ExtractedValuesFromColumn(
        data, "molecule_properties", "hbd_lipinski", is_list=False
      ),
      "heavy_atoms": ExtractedValuesFromColumn(
        data, "molecule_properties", "heavy_atoms", is_list=False
      ),
      "molecular_species": ExtractedValuesFromColumn(
        data, "molecule_properties", "molecular_species", is_list=False
      ),
      "mw_freebase": ExtractedValuesFromColumn(
        data, "molecule_properties", "mw_freebase", is_list=False
      ),
      "mw_monoisotopic": ExtractedValuesFromColumn(
        data, "molecule_properties", "mw_monoisotopic", is_list=False
      ),
      "np_likeness_score": ExtractedValuesFromColumn(
        data, "molecule_properties", "np_likeness_score", is_list=False
      ),
      "num_lipinski_ro5_violations": ExtractedValuesFromColumn(
        data, "molecule_properties", "num_lipinski_ro5_violations", is_list=False
      ),
      "num_ro5_violations": ExtractedValuesFromColumn(
        data, "molecule_properties", "num_ro5_violations", is_list=False
      ),
      "psa": ExtractedValuesFromColumn(data, "molecule_properties", "psa", is_list=False),
      "qed_weighted": ExtractedValuesFromColumn(
        data, "molecule_properties", "qed_weighted", is_list=False
      ),
      "ro3_pass": ExtractedValuesFromColumn(
        data, "molecule_properties", "ro3_pass", is_list=False
      ),
      "rtb": ExtractedValuesFromColumn(data, "molecule_properties", "rtb", is_list=False),
      # ! molecule_structures
      "canonical_smiles": ExtractedValuesFromColumn(
        data, "molecule_structures", "canonical_smiles", is_list=False
      ),
      "molfile": ExtractedValuesFromColumn(
        data, "molecule_structures", "molfile", is_list=False
      ),
      "standard_inchi": ExtractedValuesFromColumn(
        data, "molecule_structures", "standard_inchi", is_list=False
      ),
      "standard_inchi_key": ExtractedValuesFromColumn(
        data, "molecule_structures", "standard_inchi_key", is_list=False
      ),
      # ! molecule_synonyms
      "molecule_synonym": ExtractedValuesFromColumn(
        data, "molecule_synonyms", "molecule_synonym"
      ),
      "syn_type": ExtractedValuesFromColumn(data, "molecule_synonyms", "syn_type"),
      "synonyms": ExtractedValuesFromColumn(data, "molecule_synonyms", "synonyms"),
    }
  )

  # удаляем исходные столбцы со словарями и списками словарей.
  data = data.drop(
    [
      "cross_references",
      "molecule_hierarchy",
      "molecule_properties",
      "molecule_structures",
      "molecule_synonyms",
    ],
    axis=1,
  )

  # объединяем исходный DataFrame с извлеченными значениями.
  return pd.concat([data, exposed_data], axis=1)


@ReTry(attempts_amount=1)
def DownloadCompoundsByMWRange(
  less_limit: int, greater_limit: int, results_folder_name: str
):
  """
  Возвращает молекулы в диапазоне молекулярной массы [less_limit;
  greater_limit) из базы ChEMBL, сохраняя их в .csv файл.

  Args:
      less_limit (int): нижняя граница.
      greater_limit (int): верхняя граница.
      results_folder_name (str): имя папки для закачки.
  """

  v_logger.info(
    f"Downloading molecules with mw in range [{less_limit}, {greater_limit})...",
    LogMode.VERBOSELY,
  )

  # получаем молекулы в заданном диапазоне молекулярной массы.
  mols_in_mw_range: QuerySet = QuerySetCompoundsByMWRange(less_limit, greater_limit)

  v_logger.info(
    f"Amount: {len(mols_in_mw_range)}",  # type: ignore
    LogMode.VERBOSELY,
  )
  v_logger.success(
    f"Downloading molecules with mw in range [{less_limit}, {greater_limit})!",
    LogMode.VERBOSELY,
  )

  v_logger.info("Collecting molecules to pandas.DataFrame...", LogMode.VERBOSELY)

  # преобразуем данные в DataFrame.
  data_frame = ExpandedFromDictionariesCompoundsDF(pd.DataFrame(mols_in_mw_range))  # type: ignore

  v_logger.success("Collecting molecules to pandas.DataFrame!", LogMode.VERBOSELY)
  v_logger.info(
    f"Collecting molecules to .csv file in '{results_folder_name}'...", LogMode.VERBOSELY
  )

  # формируем имя файла для сохранения.
  file_name: str = f"{results_folder_name}/range_{less_limit}_{greater_limit}_mw_mols.csv"

  # сохраняем DataFrame в .csv файл.
  data_frame.to_csv(file_name, sep=";", index=False)

  v_logger.success(
    f"Collecting molecules to .csv file in '{results_folder_name}'!", LogMode.VERBOSELY
  )


def SaveChEMBLMolfilesToSDFByIdList(
  molecule_chembl_id_list: list[str],
  file_name: str,
  extra_data: pd.DataFrame = pd.DataFrame(),
):
  """
  Сохраняет molfiles из списка id в .sdf файл.

  Args:
      molecule_chembl_id_list (list[str]): список id.
      file_name (str): имя файла (без .sdf).
      extra_data (pd.DataFrame): дополнительная информация.
  """

  # если список molecule_chembl_id пуст.
  if not molecule_chembl_id_list:
    v_logger.warning(
      "Molecules list is empty, nothing to save to .sdf!", LogMode.VERBOSELY
    )
    return

  @ReTry()
  def DataFrameMolfilesFromIdList(molecule_chembl_id_list: list[str]) -> pd.DataFrame:
    """
    Возвращает pd.DataFrame из molfile по каждой молекуле из списка
    molecule_chembl_id.

    Args:
        molecule_chembl_id_list (list[str]): список id.

    Returns:
        pd.DataFrame: DataFrame, который содержит molecule_chembl_id и
                      соотв. molfile.
    """

    # фильтруем молекулы по списку id.
    qs_data: QuerySet = new_client.molecule.filter(  # type: ignore
      molecule_chembl_id__in=molecule_chembl_id_list
    ).only(["molecule_chembl_id", "molecule_structures"])

    data = pd.DataFrame(qs_data)  # type: ignore

    # извлекаем molfile из структуры молекулы.
    data["molfile"] = data["molecule_structures"].apply(
      lambda x: x["molfile"] if isinstance(x, dict) else None
    )

    # удаляем столбец molecule_structures.
    data = data.drop(["molecule_structures"], axis=1)

    return data

  v_logger.info("Collecting molfiles to pandas.DataFrame...", LogMode.VERBOSELY)

  # получаем DataFrame из molfiles.
  data = DataFrameMolfilesFromIdList(molecule_chembl_id_list)

  v_logger.success("Collecting molfiles to pandas.DataFrame!", LogMode.VERBOSELY)

  # сохраняем molfiles в .sdf файл.
  SaveMolfilesToSDF(
    data=data,
    file_name=file_name,
    molecule_id_column_name="molecule_chembl_id",
    extra_data=extra_data,
  )
