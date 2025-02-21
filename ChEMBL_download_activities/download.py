from ChEMBL_download_activities.functions import *
from ChEMBL_download_compounds.functions import SaveMolfilesToSDFByIdList

from Utils.decorators import IgnoreWarnings, ReTry
from Utils.files_funcs import json, IsFileInFolder, os
from Utils.verbose_logger import v_logger, LogMode

from Configurations.config import Config, LoggerConfig


@IgnoreWarnings
@ReTry(attempts_amount=1)
def DownloadTargetChEMBLActivities(targets_data: pd.DataFrame,
                                   config: Config):
    """
    Скачивает информацию об активностях, связанных с заданными мишенями,
    из базы данных ChEMBL на основе конфигурации (`config.json`).

    Args:
        targets_data (pd.DataFrame): DataFrame, содержащий информацию о мишенях.
        config (Config): словарь, содержащий параметры конфигурации для процесса скачивания.
    """

    activities_config: Config = config["ChEMBL_download_activities"]
    compounds_config: Config = config["ChEMBL_download_compounds"]

    v_logger.UpdateFormat(activities_config["logger_label"],
                          activities_config["logger_color"])

    v_logger.info(f"Start download activities connected with targets...")
    v_logger.info(f"{'-' * 77}", LogMode.VERBOSELY)

    for target_id in targets_data['target_chembl_id']:
        file_name_ic50: str = f"{target_id}_IC50_activities"
        file_name_ki: str = f"{target_id}_Ki_activities"

        if config["skip_downloaded"] \
           and IsFileInFolder(f"{file_name_ic50}.csv",
                              activities_config["results_folder_name"]) \
           and IsFileInFolder(f"{file_name_ki}.csv",
                              activities_config["results_folder_name"]):

            v_logger.info("Activities connected with target "
                          f"{target_id} is already downloaded, skip.",
                          LogMode.VERBOSELY)
            v_logger.info(f"{'-' * 77}", LogMode.VERBOSELY)

            continue

        v_logger.info(f"Downloading activities connected with {target_id}...",
                      LogMode.VERBOSELY)

        activities_ic50: QuerySet = QuerySetActivitiesByIC50(target_id)
        activities_ki: QuerySet = QuerySetActivitiesByKi(target_id)

        v_logger.info("Amount: IC50: "
                      f"{len(activities_ic50)};"  # type: ignore
                      " Ki: "
                      f"{len(activities_ki)}.", LogMode.VERBOSELY)  # type: ignore
        v_logger.success(f"Downloading activities connected with {target_id}!",
                         LogMode.VERBOSELY)
        v_logger.info("Collecting activities to pandas.DataFrame...",
                      LogMode.VERBOSELY)

        data_frame_ic50 = CleanedTargetActivitiesDF(
            pd.DataFrame(activities_ic50),  # type: ignore
            target_id=target_id,
            activities_type="IC50")

        data_frame_ki = CleanedTargetActivitiesDF(
            pd.DataFrame(activities_ki),  # type: ignore
            target_id=target_id,
            activities_type="Ki")

        v_logger.success("Collecting activities to pandas.DataFrame!",
                         LogMode.VERBOSELY)
        v_logger.info(
            "Recording new values 'IC50', 'Ki' in targets DataFrame...",
            LogMode.VERBOSELY)

        targets_data.loc[targets_data["target_chembl_id"] == target_id, "IC50_new"] =\
            len(data_frame_ic50)

        targets_data.loc[targets_data["target_chembl_id"] == target_id, "Ki_new"] =\
            len(data_frame_ki)

        v_logger.info("Amount: IC50: "
                      f"{len(data_frame_ic50)};"
                      " Ki: "
                      f"{len(data_frame_ki)}.", LogMode.VERBOSELY)
        v_logger.success(
            "Recording new values 'IC50', 'Ki' in targets DataFrame!",
            LogMode.VERBOSELY)
        v_logger.info(
            "Collecting activities to .csv file in "
            f"'{activities_config["results_folder_name"]}'...",
            LogMode.VERBOSELY)

        full_file_name_ic50: str = f"{activities_config["results_folder_name"]}/"\
            f"{file_name_ic50}.csv"
        full_file_name_ki: str = f"{activities_config["results_folder_name"]}/"\
            f"{file_name_ki}.csv"

        data_frame_ic50.to_csv(full_file_name_ic50, sep=";", index=False)
        data_frame_ki.to_csv(full_file_name_ki, sep=";", index=False)

        v_logger.success(
            "Collecting activities to .csv file in "
            f"'{activities_config["results_folder_name"]}'!",
            LogMode.VERBOSELY)

        if activities_config["download_compounds_sdf"]:
            v_logger.UpdateFormat(compounds_config["logger_label"],
                                  compounds_config["logger_color"])

            v_logger.info(
                f"Start download molfiles connected with {target_id} to .sdf...",
                LogMode.VERBOSELY)

            os.makedirs(compounds_config["molfiles_folder_name"], exist_ok=True)

            v_logger.info("Saving connected with IC50 molfiles...",
                          LogMode.VERBOSELY)

            SaveMolfilesToSDFByIdList(
                data_frame_ic50['molecule_chembl_id'].tolist(),
                f"{compounds_config["molfiles_folder_name"]}/{file_name_ic50}_molfiles",
                extra_data=data_frame_ic50)

            v_logger.success("Saving connected with IC50 molfiles!",
                             LogMode.VERBOSELY)
            v_logger.info("Saving connected with Ki molfiles...",
                          LogMode.VERBOSELY)

            SaveMolfilesToSDFByIdList(
                data_frame_ki['molecule_chembl_id'].tolist(),
                f"{compounds_config["molfiles_folder_name"]}/{file_name_ki}_molfiles",
                extra_data=data_frame_ki)

            v_logger.success("Saving connected with Ki molfiles!", LogMode.VERBOSELY)
            v_logger.success(
                f"End download molfiles connected with {target_id} to .sdf!",
                LogMode.VERBOSELY)

            v_logger.UpdateFormat(activities_config["logger_label"],
                                  activities_config["logger_color"])

        v_logger.info(f"{'-' * 77}", LogMode.VERBOSELY)

    v_logger.success("End download activities connected with targets!")


@IgnoreWarnings
@ReTry(attempts_amount=1)
def GetCellLineChEMBLActivitiesFromCSV(cell_lines_data: pd.DataFrame,
                                       config: Config):
    """
    "Скачивает" (получает) информацию об активностях, связанных с
    заданными клеточными линиями, из базы данных ChEMBL на основе
    конфигурации (`config.json`).

    Args:
        targets_data (pd.DataFrame): DataFrame, содержащий информацию о клеточных линиях.
        config (Config): словарь, содержащий параметры конфигурации для процесса скачивания.
    """

    activities_config: Config = config["ChEMBL_download_activities"]
    cell_lines_config: Config = config["ChEMBL_download_cell_lines"]
    compounds_config: Config = config["ChEMBL_download_compounds"]

    restore_index: int = v_logger.UpdateFormat(activities_config["logger_label"],
                                               activities_config["logger_color"]) - 1

    v_logger.info("Start getting activities connected with cell_lines...")
    v_logger.info(f"{'-' * 77}", LogMode.VERBOSELY)

    for cell_id in cell_lines_data['cell_chembl_id']:
        file_name_ic50: str = f"{cell_id}_IC50_activities"
        file_name_gi50: str = f"{cell_id}_GI50_activities"

        if config["skip_downloaded"] \
           and IsFileInFolder(f"{file_name_ic50}.csv",
                              activities_config["results_folder_name"]) \
           and IsFileInFolder(f"{file_name_gi50}.csv",
                              activities_config["results_folder_name"]):

            v_logger.info("Activities connected with target "
                          f"{cell_id} is already gotten, skip", LogMode.VERBOSELY)
            v_logger.info(f"{'-' * 77}", LogMode.VERBOSELY)

            continue

        v_logger.info(f"Getting activities connected with {cell_id}...", LogMode.VERBOSELY)

        data_frame_ic50 = pd.read_csv(
            f"{cell_lines_config["raw_csv_folder_name"]}/{file_name_ic50}.csv",
            sep=";", low_memory=False)

        data_frame_gi50 = pd.read_csv(
            f"{cell_lines_config["raw_csv_folder_name"]}/{file_name_gi50}.csv",
            sep=";", low_memory=False)

        v_logger.info("Amount: "
                      f"IC50: {len(data_frame_ic50)}; "
                      f"GI50: {len(data_frame_gi50)}.", LogMode.VERBOSELY)

        v_logger.success(f"Getting activities connected with {cell_id}!",
                         LogMode.VERBOSELY)
        v_logger.info("Cleaning activities...", LogMode.VERBOSELY)

        data_frame_ic50 = CleanedCellLineActivitiesDF(data_frame_ic50,
                                                      cell_id=cell_id,
                                                      activities_type="IC50")

        data_frame_gi50 = CleanedCellLineActivitiesDF(data_frame_gi50,
                                                      cell_id=cell_id,
                                                      activities_type="GI50")

        v_logger.success(
            "Collecting activities to pandas.DataFrame!", LogMode.VERBOSELY)
        v_logger.info(
            "Recording new values 'IC50', 'GI50' in targets DataFrame...",
            LogMode.VERBOSELY)

        cell_lines_data.loc[cell_lines_data["cell_chembl_id"]
                            == cell_id, "IC50_new"] = len(data_frame_ic50)

        cell_lines_data.loc[cell_lines_data["cell_chembl_id"]
                            == cell_id, "GI50_new"] = len(data_frame_gi50)

        v_logger.info("Amount: "
                      f"IC50: {len(data_frame_ic50)}; "
                      f"GI50: {len(data_frame_gi50)}.", LogMode.VERBOSELY)
        v_logger.success(
            "Recording new values 'IC50', 'GI50' in targets DataFrame!",
            LogMode.VERBOSELY)
        v_logger.info(
            f"Collecting activities to .csv file in '"
            f"{activities_config["results_folder_name"]}'...", LogMode.VERBOSELY)

        full_file_name_ic50: str = f"{activities_config["results_folder_name"]}/"\
            f"{file_name_ic50}.csv"
        full_file_name_gi50: str = f"{activities_config["results_folder_name"]}/"\
            f"{file_name_gi50}.csv"

        data_frame_ic50.to_csv(full_file_name_ic50, sep=";", index=False)
        data_frame_gi50.to_csv(full_file_name_gi50, sep=";", index=False)

        v_logger.success(
            f"Collecting activities to .csv file in "
            f"'{activities_config["results_folder_name"]}'!", LogMode.VERBOSELY)

        if activities_config["download_compounds_sdf"]:
            v_logger.UpdateFormat(compounds_config["logger_label"],
                                  compounds_config["logger_color"])

            v_logger.info(
                f"Start download molfiles connected with {cell_id} to .sdf...",
                LogMode.VERBOSELY)

            os.makedirs(compounds_config["molfiles_folder_name"], exist_ok=True)

            v_logger.info("Saving connected with IC50 molfiles...",
                          LogMode.VERBOSELY)

            SaveMolfilesToSDFByIdList(
                data_frame_ic50['molecule_chembl_id'].tolist(),
                f"{compounds_config["molfiles_folder_name"]}/{file_name_ic50}_molfiles",
                extra_data=data_frame_ic50)

            v_logger.success("Saving connected with IC50 molfiles!",
                             LogMode.VERBOSELY)
            v_logger.info("Saving connected with GI50 molfiles...",
                          LogMode.VERBOSELY)

            SaveMolfilesToSDFByIdList(
                data_frame_gi50['molecule_chembl_id'].tolist(),
                f"{compounds_config["molfiles_folder_name"]}/{file_name_gi50}_molfiles",
                extra_data=data_frame_gi50)

            v_logger.success(
                "Saving connected with GI50 molfiles!", LogMode.VERBOSELY)
            v_logger.success(
                f"End download molfiles connected with {cell_id} to .sdf!",
                LogMode.VERBOSELY)

        v_logger.UpdateFormat(activities_config["logger_label"],
                              activities_config["logger_color"])

        v_logger.info(f"{'-' * 77}", LogMode.VERBOSELY)

    v_logger.success("End getting activities connected with cell_lines!")

    v_logger.RestoreFormat(restore_index)
