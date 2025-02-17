# from icecream import ic

from ChEMBL_download_activities.functions import *

from ChEMBL_download_compounds.functions import SaveMolfilesToSDFByIdList

from Utils.decorators import IgnoreWarnings

# ic.disable()


@IgnoreWarnings
def DownloadTargetChEMBLActivities(targets_data: pd.DataFrame,
                                   config: dict):
    """
    Скачивает информацию об активностях, связанных с заданными мишенями,
    из базы данных ChEMBL на основе конфигурации (`config.json`).

    Args:
        targets_data (pd.DataFrame): DataFrame, содержащий информацию о мишенях.
        config (dict): словарь, содержащий параметры конфигурации для процесса скачивания.
    """

    activities_config = config["ChEMBL_download_activities"]
    compounds_config = config["ChEMBL_download_compounds"]

    UpdateLoggerFormat(activities_config["logger_label"], activities_config["logger_color"])

    logger.info(
        f"Start download activities connected with targets...")

    logger.info(f"{'-' * 77}")

    for target_id in targets_data['target_chembl_id']:
        file_name_ic50: str = f"{target_id}_IC50_activities"
        file_name_ki: str = f"{target_id}_Ki_activities"

        if config["skip_downloaded"] \
           and IsFileInFolder(activities_config["results_folder_name"], f"{file_name_ic50}.csv") \
           and IsFileInFolder(activities_config["results_folder_name"], f"{file_name_ki}.csv"):
            logger.warning("Activities connected with target "
                           f"{target_id} is already downloaded, skip")
            logger.info(f"{'-' * 77}")

            continue

        if config["print_to_console_verbosely"]:
            logger.info("Downloading activities connected with "
                        f"{target_id}...")

        activities_ic50: QuerySet = QuerySetActivitiesByIC50(target_id)
        activities_ki: QuerySet = QuerySetActivitiesByKi(target_id)

        if config["print_to_console_verbosely"]:
            logger.info("Amount: IC50: "
                        f"{len(activities_ic50)};"  # type: ignore
                        " Ki:"
                        f"{len(activities_ki)}")  # type: ignore

            logger.success("Downloading activities connected with "
                           f"{target_id}!")

            logger.info(
                "Collecting activities to pandas.DataFrame()...")

        try:
            data_frame_ic50 = CleanedTargetActivitiesDF(
                pd.DataFrame(activities_ic50),  # type: ignore
                target_id=target_id,
                activities_type="IC50",
                print_to_console=config["print_to_console_verbosely"])

            data_frame_ki = CleanedTargetActivitiesDF(
                pd.DataFrame(activities_ki),  # type: ignore
                target_id=target_id,
                activities_type="Ki",
                print_to_console=config["print_to_console_verbosely"])

            if config["print_to_console_verbosely"]:
                logger.success(
                    "Collecting activities to pandas.DataFrame()!")

                logger.info(
                    "Recording new values 'IC50', 'Ki' in targets DataFrame...")

            targets_data.loc[targets_data["target_chembl_id"] == target_id, "IC50_new"] =\
                len(data_frame_ic50)

            targets_data.loc[targets_data["target_chembl_id"] == target_id, "Ki_new"] =\
                len(data_frame_ki)

            if config["print_to_console_verbosely"]:
                logger.info(
                    f"Amount: IC50: {len(data_frame_ic50)}; Ki: {len(data_frame_ki)}")

                logger.success(
                    "Recording new values 'IC50', 'Ki' in targets DataFrame!")

                logger.info(
                    f"Collecting activities to .csv file in '{activities_config["results_folder_name"]}'...")

            full_file_name_ic50: str = f"{activities_config["results_folder_name"]}/{file_name_ic50}.csv"
            full_file_name_ki: str = f"{activities_config["results_folder_name"]}/{file_name_ki}.csv"

            data_frame_ic50.to_csv(full_file_name_ic50, sep=';', index=False)
            data_frame_ki.to_csv(full_file_name_ki, sep=';', index=False)

            if config["print_to_console_verbosely"]:
                logger.success(
                    f"Collecting activities to .csv file in '{activities_config["results_folder_name"]}'!")

            if activities_config["download_compounds_sdf"]:
                if config["print_to_console_verbosely"]:
                    UpdateLoggerFormat(compounds_config["logger_label"],
                                       compounds_config["logger_color"])

                    logger.info(
                        f"Start download molfiles connected with {target_id} to .sdf...")

                CreateFolder(compounds_config["molfiles_folder_name"], "molfiles")

                if config["print_to_console_verbosely"]:
                    logger.info(
                        "Saving connected with IC50 molfiles...")

                try:
                    SaveMolfilesToSDFByIdList(
                        data_frame_ic50['molecule_chembl_id'].tolist(),
                        f"{compounds_config["molfiles_folder_name"]}/{file_name_ic50}_molfiles",
                        extra_data=data_frame_ic50,
                        print_to_console=config["print_to_console_verbosely"])

                    if config["print_to_console_verbosely"]:
                        logger.success(
                            "Saving connected with IC50 molfiles")

                except Exception as exception:
                    LogException(exception)

                if config["print_to_console_verbosely"]:
                    logger.info(
                        "Saving connected with Ki molfiles...")

                try:
                    SaveMolfilesToSDFByIdList(
                        data_frame_ki['molecule_chembl_id'].tolist(),
                        f"{compounds_config["molfiles_folder_name"]}/{file_name_ki}_molfiles",
                        extra_data=data_frame_ki,
                        print_to_console=config["print_to_console_verbosely"])

                    if config["print_to_console_verbosely"]:
                        logger.success(
                            "Saving connected with Ki molfiles")

                        logger.success(
                            f"End download molfiles connected with {target_id} to .sdf")

                except Exception as exception:
                    LogException(exception)

                UpdateLoggerFormat(activities_config["logger_label"],
                                   activities_config["logger_color"])

            if config["print_to_console_verbosely"]:
                logger.info(f"{'-' * 77}")

        except Exception as exception:
            LogException(exception)

    logger.success(
        f"End download activities connected with targets!")


@IgnoreWarnings
def GetCellLineChEMBLActivitiesFromCSV(cell_lines_data: pd.DataFrame,
                                       config: dict):
    """
    "Скачивает" (получает) информацию об активностях, связанных с
    заданными клеточными линиями, из базы данных ChEMBL на основе
    конфигурации (`config.json`).

    Args:
        targets_data (pd.DataFrame): DataFrame, содержащий информацию о клеточных линиях.
        config (dict): словарь, содержащий параметры конфигурации для процесса скачивания.
    """

    activities_config = config["ChEMBL_download_activities"]
    cell_lines_config = config["ChEMBL_download_cell_lines"]
    compounds_config = config["ChEMBL_download_compounds"]

    UpdateLoggerFormat(activities_config["logger_label"], activities_config["logger_color"])

    logger.info(
        f"Start getting activities connected with cell lines...")

    logger.info(f"{'-' * 77}")

    for cell_id in cell_lines_data['cell_chembl_id']:
        file_name_ic50: str = f"{cell_id}_IC50_activities"
        file_name_gi50: str = f"{cell_id}_GI50_activities"

        if config["skip_downloaded"] \
           and IsFileInFolder(activities_config["results_folder_name"], f"{file_name_ic50}.csv") \
           and IsFileInFolder(activities_config["results_folder_name"], f"{file_name_gi50}.csv"):
            logger.warning("Activities connected with target "
                           f"{cell_id} is already gotten, skip")
            logger.info(f"{'-' * 77}")

            continue

        if config["print_to_console_verbosely"]:
            logger.info(f"Getting activities connected with {cell_id}...")

        data_frame_ic50 = pd.read_csv(
            f"{cell_lines_config["raw_csv_folder_name"]}/{file_name_ic50}.csv",
            sep=';', low_memory=False)

        data_frame_gi50 = pd.read_csv(
            f"{cell_lines_config["raw_csv_folder_name"]}/{file_name_gi50}.csv",
            sep=';', low_memory=False)

        if config["print_to_console_verbosely"]:
            logger.info(f"Amount: IC50: {len(data_frame_ic50)}; "
                        f"GI50: {len(data_frame_gi50)}")

            logger.success("Getting activities connected with {cell_id}!")

            logger.info(
                "Cleaning activities...")

        try:
            data_frame_ic50 = CleanedCellLineActivitiesDF(data_frame_ic50,
                                                          cell_id=cell_id,
                                                          activities_type="IC50",
                                                          print_to_console=config["print_to_console_verbosely"])

            data_frame_gi50 = CleanedCellLineActivitiesDF(data_frame_gi50,
                                                          cell_id=cell_id,
                                                          activities_type="GI50",
                                                          print_to_console=config["print_to_console_verbosely"])

            if config["print_to_console_verbosely"]:
                logger.success(
                    "Collecting activities to pandas.DataFrame()!")

                logger.info(
                    "Recording new values 'IC50', 'GI50' in targets DataFrame...")

            cell_lines_data.loc[cell_lines_data["cell_chembl_id"]
                                == cell_id, "IC50_new"] = len(data_frame_ic50)

            cell_lines_data.loc[cell_lines_data["cell_chembl_id"]
                                == cell_id, "GI50_new"] = len(data_frame_gi50)

            if config["print_to_console_verbosely"]:
                logger.info(f"Amount: IC50: {len(data_frame_ic50)}; "
                            f"GI50: {len(data_frame_gi50)}")

                logger.success(
                    "Recording new values 'IC50', 'GI50' in targets DataFrame!")

                logger.info(
                    f"Collecting activities to .csv file in '{activities_config["results_folder_name"]}'...")

            full_file_name_ic50: str = f"{activities_config["results_folder_name"]}/{file_name_ic50}.csv"
            full_file_name_gi50: str = f"{activities_config["results_folder_name"]}/{file_name_gi50}.csv"

            data_frame_ic50.to_csv(full_file_name_ic50, sep=';', index=False)
            data_frame_gi50.to_csv(full_file_name_gi50, sep=';', index=False)

            if config["print_to_console_verbosely"]:
                logger.success(
                    f"Collecting activities to .csv file in '{activities_config["results_folder_name"]}'!")

            if activities_config["download_compounds_sdf"]:
                if config["print_to_console_verbosely"]:
                    UpdateLoggerFormat(
                        compounds_config["logger_label"], compounds_config["logger_color"])

                    logger.info(
                        f"Start download molfiles connected with {cell_id} to .sdf...")

                CreateFolder(compounds_config["molfiles_folder_name"], "molfiles")

                if config["print_to_console_verbosely"]:
                    logger.info(
                        "Saving connected with IC50 molfiles...")

                try:
                    SaveMolfilesToSDFByIdList(
                        data_frame_ic50['molecule_chembl_id'].tolist(),
                        f"{compounds_config["molfiles_folder_name"]}/{file_name_ic50}_molfiles",
                        extra_data=data_frame_ic50,
                        print_to_console=config["print_to_console_verbosely"])

                    if config["print_to_console_verbosely"]:
                        logger.success(
                            "Saving connected with IC50 molfiles")

                except Exception as exception:
                    LogException(exception)

                if config["print_to_console_verbosely"]:
                    logger.info(
                        "Saving connected with GI50 molfiles...")

                try:
                    SaveMolfilesToSDFByIdList(
                        data_frame_gi50['molecule_chembl_id'].tolist(),
                        f"{compounds_config["molfiles_folder_name"]}/{file_name_gi50}_molfiles",
                        extra_data=data_frame_gi50,
                        print_to_console=config["print_to_console_verbosely"])

                    if config["print_to_console_verbosely"]:
                        logger.success(
                            "Saving connected with GI50 molfiles")

                        logger.success(
                            f"End download molfiles connected with {cell_id} to .sdf")

                except Exception as exception:
                    LogException(exception)

            UpdateLoggerFormat(activities_config["logger_label"], activities_config["logger_color"])

            if config["print_to_console_verbosely"]:
                logger.info(f"{'-' * 77}")

        except Exception as exception:
            LogException(exception)

    logger.success(
        f"End download activities connected with cell lines!")
