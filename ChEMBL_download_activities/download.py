# type: ignore

# from icecream import ic

from ChEMBL_download_activities.functions import *

from ChEMBL_download_compounds.functions import SaveMolfilesToSDFByIdList

from Utils.decorators import IgnoreWarnings

# ic.disable()


@IgnoreWarnings
def DownloadChEMBLActivities(targets_data: pd.DataFrame,
                             results_folder_name: str = "results/activities",
                             download_compounds_sdf: bool = True,
                             print_to_console: bool = False,
                             skip_downloaded_activities: bool = False) -> None:
    """
    Скачивает необходимые activities из базы ChEMBL по данным по целям.

    Args:
        targets_data (pd.DataFrame): данные по целям.
        results_folder_name (str, optional): имя папки для закачки. Defaults to "results/activities".
        download_compounds_sdf (bool, optional): нужно ли скачивать .sdf файл с molfile для каждой молекулы. Defaults to True.
        print_to_console (bool, optional): нужно ли выводить логирование в консоль. Defaults to False.
        skip_downloaded_activities (bool, optional): пропускать ли уже скачанные файлы activities. Defaults to False.
    """

    UpdateLoggerFormat("ChEMBL__IC50&Ki", "fg #61B78C")

    logger.info(
        f"Start download activities connected with targets...".ljust(77))

    logger.info(f"{'-' * 77}")

    for target_id in targets_data['target_chembl_id']:
        file_name_ic50: str = f"{target_id}_IC50_activities"
        file_name_ki: str = f"{target_id}_Ki_activities"

        if skip_downloaded_activities \
           and IsFileInFolder(results_folder_name, f"{file_name_ic50}.csv") \
           and IsFileInFolder(results_folder_name, f"{file_name_ki}.csv"):
            logger.warning(f"Activities connected with target {
                target_id} is already downloaded, skip".ljust(77))
            logger.info(f"{'-' * 77}")

            continue

        if print_to_console:
            logger.info(f"Downloading activities connected with {
                target_id}...".ljust(77))

        activities_ic50: QuerySet = QuerySetActivitiesByIC50(target_id)
        activities_ki: QuerySet = QuerySetActivitiesByKi(target_id)

        if print_to_console:
            logger.info(f"Amount: IC50: {CountActivitiesByIC50(target_id)}; Ki: {
                        CountActivitiesByKi(target_id)}".ljust(77))

            logger.success(f"Downloading activities connected with {
                target_id}: SUCCESS".ljust(77))

            logger.info(
                "Collecting activities to pandas.DataFrame()...".ljust(77))

        try:
            data_frame_ic50 = CleanedActivitiesDF(pd.DataFrame(
                activities_ic50), target_id=target_id, activities_type="IC50",
                print_to_console=print_to_console)

            data_frame_ki = CleanedActivitiesDF(pd.DataFrame(
                activities_ki), target_id=target_id, activities_type="Ki",
                print_to_console=print_to_console)

            if print_to_console:
                logger.success(
                    "Collecting activities to pandas.DataFrame(): SUCCESS".ljust(77))

                logger.info(
                    "Recording new values 'IC50', 'Ki' in targets DataFrame...".ljust(77))

            targets_data.loc[targets_data["target_chembl_id"]
                             == target_id, "IC50_new"] = len(data_frame_ic50)

            targets_data.loc[targets_data["target_chembl_id"]
                             == target_id, "Ki_new"] = len(data_frame_ki)

            if print_to_console:
                logger.info(f"Amount: IC50: {len(data_frame_ic50)}; Ki: {
                            len(data_frame_ki)}".ljust(77))

                logger.success(
                    "Recording new values 'IC50', 'Ki' in targets DataFrame: SUCCESS".ljust(77))

                logger.info(
                    f"Collecting activities to .csv file in '{results_folder_name}'...".ljust(77))

            # if need_primary_analysis:
            #     DataAnalysisByColumns(data_frame,
            #                           f"targets_data_from_ChEMBL",
            #                           f"{results_folder_name}/{primary_analysis_folder_name}")

            full_file_name_ic50: str = f"{
                results_folder_name}/{file_name_ic50}.csv"
            full_file_name_ki: str = f"{
                results_folder_name}/{file_name_ki}.csv"

            data_frame_ic50.to_csv(full_file_name_ic50, sep=';', index=False)
            data_frame_ki.to_csv(full_file_name_ki, sep=';', index=False)

            if print_to_console:
                logger.success(
                    f"Collecting activities to .csv file in '{results_folder_name}': SUCCESS".ljust(77))

            if download_compounds_sdf:
                if print_to_console:
                    UpdateLoggerFormat("ChEMBL_compound", "fg #CCA87A")

                    logger.info(
                        f"Start download molfiles connected with {target_id} to .sdf...".ljust(77))

                CreateFolder("results/compounds", "compounds")
                CreateFolder("results/compounds/molfiles", "molfiles")

                if print_to_console:
                    logger.info(
                        "Saving connected with IC50 molfiles...".ljust(77))

                try:
                    SaveMolfilesToSDFByIdList(
                        data_frame_ic50['molecule_chembl_id'].tolist(),
                        f"results/compounds/molfiles/{
                            file_name_ic50}_molfiles",
                        extra_data=data_frame_ic50,
                        print_to_console=print_to_console)

                    if print_to_console:
                        logger.success(
                            "Saving connected with IC50 molfiles".ljust(77))

                except Exception as exception:
                    logger.error(f"{exception}".ljust(77))

                if print_to_console:
                    logger.info(
                        "Saving connected with Ki molfiles...".ljust(77))

                try:
                    SaveMolfilesToSDFByIdList(
                        data_frame_ki['molecule_chembl_id'].tolist(),
                        f"results/compounds/molfiles/{file_name_ki}_molfiles",
                        extra_data=data_frame_ki,
                        print_to_console=print_to_console)

                    if print_to_console:
                        logger.success(
                            "Saving connected with Ki molfiles".ljust(77))

                        logger.success(
                            f"End download molfiles connected with {target_id} to .sdf".ljust(77))

                except Exception as exception:
                    logger.error(f"{exception}".ljust(77))

                UpdateLoggerFormat("ChEMBL__IC50&Ki", "fg #61B78C")

            if print_to_console:
                logger.info(f"{'-' * 77}")

        except Exception as exception:
            logger.error(f"{exception}".ljust(77))

    logger.success(
        f"End download activities connected with targets: SUCCESS".ljust(77))
