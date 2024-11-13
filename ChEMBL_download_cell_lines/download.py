# type: ignore

# from icecream import ic

from ChEMBL_download_cell_lines.functions import *

from Utils.decorators import IgnoreWarnings

# ic.disable()

results_folder_name: str = "results/cell_lines"
activities_results_folder_name: str = "results/activities"
primary_analysis_folder_name: str = "primary_analysis"
combined_file_name: str = "combined_cell_lines_data_from_ChEMBL"
logger_label: str = "ChEMBL____cells"


@IgnoreWarnings
def DownloadChEMBLCellLines(need_primary_analysis: bool = False,
                            download_all: bool = False,
                            download_activities: bool = True,
                            skip_downloaded_files: bool = False,
                            testing_flag: bool = False,
                            print_to_console_verbosely: bool = False) -> None:
    """
    Скачивает необходимые клеточные линии из базы ChEMBL

    Args:
        need_primary_analysis (bool, optional): нужен ли первичный анализ скачанных файлов. Defaults to False.
        download_all (bool, optional): скачивать ли все цели (или использовать только те, что нужны DrugDesign). Defaults to False.
        download_activities (bool, optional): скачивать ли наборы активностей к целям (по IC50 и Ki). Defaults to True.
        skip_downloaded_files (bool, optional): пропускать ли уже скачанные файлы. Defaults to False.
        testing_flag (bool, optional): спец. флаг для тестирования функционала. Defaults to False.
        print_to_console_verbosely (bool, optional): нужен ли более подробный вывод в консоль. Defaults to False.
    """

    UpdateLoggerFormat(logger_label, "fg #CB507C")

    logger.info(f"{'-' * 21} ChEMBL downloading for DrugDesign {'-' * 21}")

    CreateFolder("results")
    CreateFolder(results_folder_name)

    if need_primary_analysis:
        CreateFolder(f"{results_folder_name}/{primary_analysis_folder_name}")

    if download_activities:
        CreateFolder(activities_results_folder_name)

    logger.info(f"{'-' * 77}")

    id_list: list[str] = ["CHEMBL4295386", "CHEMBL3307781", "CHEMBL4295453", "CHEMBL4295483",
                          "CHEMBL3308509", "CHEMBL3706569", "CHEMBL3307715", "CHEMBL3307525",
                          "CHEMBL3307970", "CHEMBL4295409", "CHEMBL3307501", "CHEMBL3307364",
                          "CHEMBL3308499", "CHEMBL3307481", "CHEMBL3308021", "CHEMBL3307755",
                          "CHEMBL3307614"]

    if testing_flag:
        id_list = ["CHEMBL4295386", "CHEMBL3307781"]

    if not skip_downloaded_files or not IsFileInFolder(f"{results_folder_name}",
                                                       "cell_lines_data_from_ChEMBL.csv"):
        if download_all:
            id_list = []  # в случае пустого списка в DownloadCellLinesFromIdList скачаются все

        DownloadCellLinesFromIdList(cell_line_chembl_id_list=id_list,
                                    need_primary_analysis=need_primary_analysis,
                                    get_activities=download_activities,
                                    activities_results_folder_name=activities_results_folder_name,
                                    print_to_console=(
                                        print_to_console_verbosely or testing_flag),
                                    skip_gotten_activities=skip_downloaded_files)

    else:
        logger.warning(
            f"cell_lines_data_from_ChEMBL is already downloaded, skip".ljust(77))

    logger.success(f"{'-' * 21} ChEMBL downloading for DrugDesign {'-' * 21}")
