"""
ChEMBL_download_activities/functions.py

Этот модуль содержит функции для запроса и обработки данных об активностях
(IC50, Ki) из базы данных ChEMBL, а также для очистки данных об активностях
клеточных линий.
"""

from chembl_webresource_client.new_client import new_client
from chembl_webresource_client.query_set import QuerySet

from Configurations.config import Config, config
from Utils.dataframe_funcs import MedianDedupedDF, pd
from Utils.decorators import ReTry
from Utils.verbose_logger import LogMode, v_logger


@ReTry()
def QuerySetActivitiesByIC50(target_id: str) -> QuerySet:
  """
  Возвращает QuerySet активностей для указанной цели (target_id) на основе IC50.

  IC50 (Half maximal inhibitory concentration) - полумаксимальная ингибирующая
  концентрация.

  Args:
      target_id (str): Идентификатор цели из базы ChEMBL.

  Returns:
      QuerySet: QuerySet, содержащий активности, отфильтрованные по target_id и
                типу "IC50".

  Raises:
      Exception: Если не удается получить данные после нескольких попыток
                 (благодаря декоратору ReTry).
  """

  return new_client.activity.filter(  # type: ignore
    target_chembl_id=target_id
  ).filter(standard_type="IC50")


@ReTry()
def QuerySetActivitiesByKi(target_id: str) -> QuerySet:
  """
  Возвращает QuerySet активностей для указанной цели (target_id) на основе Ki.

  Ki (Inhibition constant) - константа ингибирования.

  Args:
      target_id (str): Идентификатор цели из базы ChEMBL.

  Returns:
      QuerySet: QuerySet, содержащий активности, отфильтрованные по target_id и
                типу "Ki".

  Raises:
      Exception: Если не удается получить данные после нескольких попыток
                 (благодаря декоратору ReTry).
  """

  return new_client.activity.filter(  # type: ignore
    target_chembl_id=target_id
  ).filter(standard_type="Ki")


def CountTargetActivitiesByIC50(target_id: str) -> int:
  """
  Подсчитывает количество активностей для указанной цели (target_id) на основе IC50.

  Args:
      target_id (str): Идентификатор цели из базы ChEMBL.

  Returns:
      int: Количество активностей типа IC50 для указанной цели.
  """

  return len(QuerySetActivitiesByIC50(target_id))  # type: ignore


def CountTargetActivitiesByKi(target_id: str) -> int:
  """
  Подсчитывает количество активностей для указанной цели (target_id) на основе Ki.

  Args:
      target_id (str): Идентификатор цели из базы ChEMBL.

  Returns:
      int: Количество активностей типа Ki для указанной цели.
  """

  return len(QuerySetActivitiesByKi(target_id))  # type: ignore


def CountCellLineActivitiesByFile(file_name: str) -> int:
  """
  Подсчитывает количество строк (активностей) в CSV-файле,
  содержащем данные о клеточных линиях.

  Args:
      file_name (str): Имя файла CSV,
                       содержащего данные об активностях клеточных линий.

  Returns:
      int: Количество строк в файле (предположительно, количество активностей).
  """

  return sum(1 for _ in open(file_name))


@ReTry(attempts_amount=1)
def CleanedTargetActivitiesDF(
  data: pd.DataFrame,
  target_id: str,
  activities_type: str,
) -> pd.DataFrame:
  """
  Очищает DataFrame с данными об активностях
  для указанной цели (target_id) по IC50 и Ki.

  Функция выполняет следующие шаги:
      1. Удаляет неинформативные столбцы.
      2. Фильтрует данные, оставляя только значения с отношением, единицами,
         организмами, типами активности и типами анализа из файла с конфигурациями.
      3. Преобразует столбец "standard_value" в числовой тип.
      4. Удаляет значения "standard_value", превышающие 1000000000 (1e9).
      5. Заменяет значения "Not Determined" в столбце 'activity_comment' на None.
      6. Удаляет столбцы "target_organism" и "standard_type".
      7. Вычисляет медиану для дублирующихся значений "standard_value"
         по "molecule_chembl_id".
      8. Переиндексирует столбцы DataFrame в логическом порядке.

  Args:
      data (pd.DataFrame): DataFrame с данными об активностях, полученными из ChEMBL.
      target_id (str): Идентификатор цели из базы ChEMBL.
      activities_type (str): Тип активности ("IC50" или "Ki")
                             (используется только для логирования).

  Returns:
      pd.DataFrame: Очищенный DataFrame с данными об активностях.
  """
  # конфигурация для фильтрации активностей (мишеней).
  filtering_config: Config = config["ChEMBL_download_activities"]["filtering"]["targets"]

  v_logger.info(
    f"Start cleaning {activities_type} activities DataFrame from {target_id}...",
    LogMode.VERBOSELY,
  )
  v_logger.info("Deleting useless columns...", LogMode.VERBOSELY)

  data = data.drop(
    [
      "activity_id",
      "activity_properties",
      "document_journal",
      "document_year",
      "molecule_pref_name",
      "pchembl_value",
      "potential_duplicate",
      "qudt_units",
      "record_id",
      "src_id",
      "standard_flag",
      "standard_text_value",
      "standard_upper_value",
      "target_chembl_id",
      "target_pref_name",
      "target_tax_id",
      "text_value",
      "toid",
      "type",
      "units",
      "uo_units",
      "upper_value",
      "value",
      "ligand_efficiency",
      "relation",
    ],
    axis=1,
  )

  v_logger.success("Deleting useless columns!", LogMode.VERBOSELY)
  v_logger.info("Deleting inappropriate elements...", LogMode.VERBOSELY)

  data = data[data["standard_relation"].isin(filtering_config["standard_relation"])]
  data = data[data["standard_units"].isin(filtering_config["standard_units"])]
  data = data[data["target_organism"].isin(filtering_config["target_organism"])]
  data = data[data["standard_type"].isin(filtering_config["standard_type"])]
  data = data[data["assay_type"].isin(filtering_config["assay_type"])]

  data["standard_value"] = data["standard_value"].astype(float)
  # неправдоподобные значения
  max_value: int = 1000000000
  data = data[data["standard_value"] <= max_value]

  data["activity_comment"] = data["activity_comment"].replace("Not Determined", None)

  data = data.drop(["target_organism", "standard_type"], axis=1)

  v_logger.success("Deleting inappropriate elements!", LogMode.VERBOSELY)
  v_logger.info("Calculating median for 'standard value'...", LogMode.VERBOSELY)

  data = MedianDedupedDF(data, "molecule_chembl_id", "standard_value")

  v_logger.success("Calculating median for 'standard value'!", LogMode.VERBOSELY)
  v_logger.info("Reindexing columns in logical order...", LogMode.VERBOSELY)

  data = data.reindex(
    columns=[
      "molecule_chembl_id",
      "parent_molecule_chembl_id",
      "canonical_smiles",
      "document_chembl_id",
      "standard_relation",
      "standard_value",
      "standard_units",
      "assay_chembl_id",
      "assay_description",
      "assay_type",
      "assay_variant_accession",
      "assay_variant_mutation",
      "action_type",
      "activity_comment",
      "data_validity_comment",
      "data_validity_description",
      "bao_endpoint",
      "bao_format",
      "bao_label",
    ]
  )

  v_logger.success("Reindexing columns in logical order!", LogMode.VERBOSELY)
  v_logger.success(
    f"End cleaning activities DataFrame from {target_id}!", LogMode.VERBOSELY
  )
  v_logger.info("-", LogMode.VERBOSELY)

  return data


@ReTry(attempts_amount=1)
def CleanedCellLineActivitiesDF(
  data: pd.DataFrame,
  cell_id: str,
  activities_type: str,
) -> pd.DataFrame:
  """
  Очищает DataFrame с данными об активностях
  для указанной клеточной линии (cell_id) по IC50 и GI50.

  Функция выполняет следующие шаги:
      1. Выбирает нужные столбцы из DataFrame.
      2. Переименовывает столбцы,
         приводя их к нижнему регистру и заменяя пробелы на "_".
      3. Фильтрует данные, оставляя только значения с отношениями, единицами,
         организмами и типами активности из файла с конфигурациями.
      4. Преобразует столбец "standard_value" в числовой тип.
      5. Удаляет значения "standard_value", превышающие 1000000000 (1e9).
      6. Удаляет столбец "assay_organism" и "standard_type".
      7. Переименовывает столбец "smiles" в "canonical_smiles".
      8. Вычисляет медиану для дублирующихся значений "standard_value"
         по "molecule_chembl_id".
      9. Переиндексирует столбцы DataFrame в логическом порядке.

  Args:
      data (pd.DataFrame): DataFrame с данными об активностях клеточных линий.
      cell_id (str): Идентификатор клеточной линии.
      activities_type (str): Тип активности ("IC50" или "GI50")
                             (используется только для логирования).

  Returns:
      pd.DataFrame: Очищенный DataFrame с данными об активностях клеточной линии.
  """

  # конфигурация для фильтрации активностей (клеточных линий).
  filtering_config: Config = config["ChEMBL_download_activities"]["filtering"][
    "cell_lines"
  ]

  v_logger.info(
    f"Start cleaning {activities_type} activities DataFrame from {cell_id}...",
    LogMode.VERBOSELY,
  )
  v_logger.info("Deleting useless columns...", LogMode.VERBOSELY)

  data = data[
    [
      "Molecule ChEMBL ID",
      "Smiles",
      "Document ChEMBL ID",
      "Standard Type",
      "Standard Relation",
      "Standard Value",
      "Standard Units",
      "Assay ChEMBL ID",
      "Assay Description",
      "Assay Type",
      "Assay Variant Accession",
      "Assay Variant Mutation",
      "Action Type",
      "Data Validity Comment",
      "BAO Format ID",
      "BAO Label",
      "Assay Organism",
    ]
  ]

  data.columns = [column_name.lower().replace(" ", "_") for column_name in data.columns]

  v_logger.success("Deleting useless columns!", LogMode.VERBOSELY)
  v_logger.info("Deleting inappropriate elements...", LogMode.VERBOSELY)

  data = data[data["standard_relation"].isin(filtering_config["standard_relation"])]
  data = data[data["standard_units"].isin(filtering_config["standard_units"])]
  data = data[data["assay_organism"].isin(filtering_config["assay_organism"])]
  data = data[data["standard_type"].isin(filtering_config["standard_type"])]

  data["standard_value"] = data["standard_value"].astype(float)

  max_value: int = 1000000000
  data = data[data["standard_value"] <= max_value]

  data = data.drop(["assay_organism", "standard_type"], axis=1)

  data = data.rename(columns={"smiles": "canonical_smiles"})

  v_logger.success("Deleting inappropriate elements!", LogMode.VERBOSELY)
  v_logger.info("Calculating median for 'standard value'...", LogMode.VERBOSELY)

  data = MedianDedupedDF(data, "molecule_chembl_id", "standard_value")

  v_logger.success("Calculating median for 'standard value'!", LogMode.VERBOSELY)
  v_logger.info("Reindexing columns in logical order...", LogMode.VERBOSELY)

  data = data.reindex(
    columns=[
      "molecule_chembl_id",
      "canonical_smiles",
      "document_chembl_id",
      "standard_relation",
      "standard_value",
      "standard_units",
      "assay_chembl_id",
      "assay_description",
      "assay_type",
      "assay_variant_accession",
      "assay_variant_mutation",
      "action_type",
      "data_validity_description",
      "bao_format",
      "bao_label",
    ]
  )

  v_logger.success("Reindexing columns in logical order!", LogMode.VERBOSELY)
  v_logger.success(
    f"End cleaning activities DataFrame from {cell_id}!", LogMode.VERBOSELY
  )
  v_logger.info("-", LogMode.VERBOSELY)

  return data
