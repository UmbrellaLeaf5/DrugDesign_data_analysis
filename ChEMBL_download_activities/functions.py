from chembl_webresource_client.new_client import new_client
from chembl_webresource_client.query_set import QuerySet

from Utils.dataframe_funcs import MedianDedupedDF, pd
from Utils.decorators import ReTry
from Utils.verbose_logger import LogMode, v_logger


@ReTry()
def QuerySetActivitiesByIC50(target_id: str) -> QuerySet:
    """
    Возвращает активности по target_id по IC50.

    Args:
        target_id (str): идентификатор цели из базы ChEMBL.

    Returns:
        QuerySet: набор активностей.
    """

    return new_client.activity.filter(  # type: ignore
        target_chembl_id=target_id).filter(standard_type="IC50")


@ReTry()
def QuerySetActivitiesByKi(target_id: str) -> QuerySet:
    """
    Возвращает активности по target_id по Ki.

    Args:
        target_id (str): идентификатор цели из базы ChEMBL.

    Returns:
        QuerySet: набор активностей.
    """

    return new_client.activity.filter(  # type: ignore
        target_chembl_id=target_id).filter(standard_type="Ki")


def CountTargetActivitiesByIC50(target_id: str) -> int:
    """
    Подсчитывает кол-во активностей по target_id по IC50.
    (иначе говоря, численное значение IC50 для конкретной цели)

    Args:
        target_id (str): идентификатор цели из базы ChEMBL.

    Returns:
        int: количество.
    """

    return len(QuerySetActivitiesByIC50(target_id))  # type: ignore


def CountTargetActivitiesByKi(target_id: str) -> int:
    """
    Подсчитывает кол-во активностей по target_id по Ki.
    (иначе говоря, численное значение Ki для конкретной цели)

    Args:
        target_id (str): идентификатор цели из базы ChEMBL.

    Returns:
        int: количество.
    """

    return len(QuerySetActivitiesByKi(target_id))  # type: ignore


def CountCellLineActivitiesByFile(file_name: str) -> int:
    """
    Подсчитывает кол-во активностей клеточных линий по .csv файлу, в котором они находятся.

    Args:
        file_name (str): имя файла.

    Returns:
        int: количество.
    """

    return sum(1 for _ in open(file_name, "r"))


@ReTry(attempts_amount=1)
def CleanedTargetActivitiesDF(data: pd.DataFrame,
                              target_id: str,
                              activities_type: str,
                              ) -> pd.DataFrame:
    """
    Производит чистку выборки activities конкретной цели по IC50 и Ki.

    Args:
        data (pd.DataFrame): выборка activities.
        target_id (str): идентификатор цели.
        activities_type (str): IC50 или Ki.

    Returns:
        pd.DataFrame: очищенная выборка
    """

    v_logger.info(f"Start cleaning {activities_type} activities DataFrame from "
                  f"{target_id}...", LogMode.VERBOSELY)
    v_logger.info(f"Deleting useless columns...", LogMode.VERBOSELY)

    data = data.drop(["activity_id", "activity_properties",
                      "document_journal", "document_year",
                      "molecule_pref_name", "pchembl_value",
                      "potential_duplicate", "qudt_units",
                      "record_id", "src_id", "standard_flag",
                      "standard_text_value", "standard_upper_value",
                      "target_chembl_id", "target_pref_name",
                      "target_tax_id", "text_value", "toid",
                      "type", "units", "uo_units", "upper_value",
                      "value", "ligand_efficiency", "relation"], axis=1)

    v_logger.success("Deleting useless columns!", LogMode.VERBOSELY)
    v_logger.info("Deleting inappropriate elements...", LogMode.VERBOSELY)

    data = data[data["standard_relation"] == "="]
    data = data[data["standard_units"] == "nM"]
    data = data[data["target_organism"] == "Homo sapiens"]
    data = data[data["standard_type"].isin(["IC50", "Ki"])]
    data = data[data["assay_type"] == "B"]

    data["standard_value"] = data["standard_value"].astype(float)
    # неправдоподобные значения
    data = data[data["standard_value"] <= 1000000000]

    data['activity_comment'] = data['activity_comment'].replace(
        "Not Determined", None)

    data = data.drop(["target_organism", "standard_type"], axis=1)

    v_logger.success("Deleting inappropriate elements!", LogMode.VERBOSELY)
    v_logger.info("Calculating median for 'standard value'...", LogMode.VERBOSELY)

    data = MedianDedupedDF(data, "molecule_chembl_id", "standard_value")

    v_logger.success("Calculating median for 'standard value'!", LogMode.VERBOSELY)
    v_logger.info("Reindexing columns in logical order...", LogMode.VERBOSELY)

    data = data.reindex(columns=["molecule_chembl_id", "parent_molecule_chembl_id",
                                 "canonical_smiles", "document_chembl_id",
                                 "standard_relation", "standard_value", "standard_units",
                                 "assay_chembl_id", "assay_description", "assay_type",
                                 "assay_variant_accession", "assay_variant_mutation",
                                 "action_type", "activity_comment",
                                 "data_validity_comment", "data_validity_description",
                                 "bao_endpoint", "bao_format", "bao_label"])

    v_logger.success("Reindexing columns in logical order!", LogMode.VERBOSELY)
    v_logger.success(f"End cleaning activities DataFrame from {target_id}!",
                     LogMode.VERBOSELY)
    v_logger.info(f"{'-' * 77}", LogMode.VERBOSELY)

    return data


@ReTry(attempts_amount=1)
def CleanedCellLineActivitiesDF(data: pd.DataFrame,
                                cell_id: str,
                                activities_type: str,
                                ) -> pd.DataFrame:
    """
    Производит чистку выборки activities конкретной клеточной линии по IC50 и GI50.

    Args:
        data (pd.DataFrame): выборка activities.
        cell_id (str): идентификатор клеточной линии.
        activities_type (str): IC50 или GI50.

    Returns:
        pd.DataFrame: очищенная выборка
    """

    v_logger.info(f"Start cleaning {activities_type} activities DataFrame from "
                  f"{cell_id}...", LogMode.VERBOSELY)
    v_logger.info("Deleting useless columns...", LogMode.VERBOSELY)

    data = data[["Molecule ChEMBL ID", "Smiles", "Document ChEMBL ID",
                 "Standard Type", "Standard Relation", "Standard Value",
                 "Standard Units", "Assay ChEMBL ID", "Assay Description",
                 "Assay Type", "Assay Variant Accession", "Assay Variant Mutation",
                 "Action Type", "Data Validity Comment", "BAO Format ID", "BAO Label",
                 "Assay Organism"]]

    data.columns = [column_name.lower().replace(" ", "_")
                    for column_name in data.columns]

    v_logger.success("Deleting useless columns!", LogMode.VERBOSELY)
    v_logger.info("Deleting inappropriate elements...", LogMode.VERBOSELY)

    data = data[data["standard_relation"] == "'='"]
    data["standard_relation"] = data["standard_relation"].str.replace(
        "'='", "=")

    data = data[data['standard_units'] == "nM"]
    data = data[data['assay_organism'] == "Homo sapiens"]
    data = data[data['standard_type'].isin(["IC50", "GI50"])]

    data['standard_value'] = data['standard_value'].astype(float)
    data = data[data['standard_value'] <= 1000000000]

    data = data.drop(["assay_organism", "standard_type"], axis=1)

    data = data.rename(columns={'smiles': "canonical_smiles"})

    v_logger.success("Deleting inappropriate elements!", LogMode.VERBOSELY)
    v_logger.info("Calculating median for 'standard value'...", LogMode.VERBOSELY)

    data = MedianDedupedDF(data, "molecule_chembl_id", "standard_value")

    v_logger.success("Calculating median for 'standard value'!", LogMode.VERBOSELY)
    v_logger.info("Reindexing columns in logical order...", LogMode.VERBOSELY)

    data = data.reindex(columns=["molecule_chembl_id",
                                 "canonical_smiles", "document_chembl_id",
                                 "standard_relation", "standard_value", "standard_units",
                                 "assay_chembl_id", "assay_description", "assay_type",
                                 "assay_variant_accession", "assay_variant_mutation",
                                 "action_type", "data_validity_description",
                                 "bao_format", "bao_label"])

    v_logger.success("Reindexing columns in logical order!", LogMode.VERBOSELY)
    v_logger.success(f"End cleaning activities DataFrame from {cell_id}!",
                     LogMode.VERBOSELY)
    v_logger.info(f"{'-' * 77}", LogMode.VERBOSELY)

    return data
