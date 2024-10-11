# type: ignore

# from icecream import ic

from ChEMBL_download_activities.functions import *

# ic.disable()


def DownloadChEMBLActivities(targets_data: pd.DataFrame,
                             results_folder_name: str = "results/activities",
                             print_to_console: bool = False) -> None:
    UpdateLoggerFormat("ChEMBL__IC50&Ki", "green")

    logger.info(
        f"Start download activities connected with targets...".ljust(77))

    logger.info(f"{'-' * 77}")

    for target_id in targets_data['target_chembl_id']:
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
            data_frame_ic50 = pd.DataFrame(activities_ic50)
            data_frame_ki = pd.DataFrame(activities_ki)

            if print_to_console:
                logger.success(
                    "Collecting activities to pandas.DataFrame(): SUCCESS".ljust(77))
                logger.info(
                    f"Collecting activities to .csv file in '{results_folder_name}'...".ljust(77))

            # if need_primary_analysis:
            #     DataAnalysisByColumns(data_frame,
            #                           f"targets_data_from_ChEMBL",
            #                           f"{results_folder_name}/{primary_analysis_folder_name}")

            file_name_ic50: str = f"{
                results_folder_name}/{target_id}_IC50_activities.csv"

            file_name_ki: str = f"{
                results_folder_name}/{target_id}_Ki_activities.csv"

            data_frame_ic50.to_csv(file_name_ic50, index=False)
            data_frame_ki.to_csv(file_name_ki, index=False)

            if print_to_console:
                logger.success(
                    f"Collecting activities to .csv file in '{results_folder_name}': SUCCESS".ljust(77))
                logger.info(f"{'-' * 77}")

        except Exception as exception:
            logger.error(f"{exception}".ljust(77))

    logger.success(
        f"End download activities connected with targets: SUCCESS".ljust(77))
