from PubChem_download_toxicity.functions import *

from Utils.logger_funcs import logger, UpdateLoggerFormat
from Utils.files_funcs import CombineCSVInFolder, DeleteFilesInFolder, \
    MoveFileToFolder, os

from Configurations.config import Config


@ReTry(attempts_amount=1)
def DownloadPubChemCompoundsToxicity(config: Config):
    """
    Скачивает информацию о токсичности соединений из базы данных PubChem 
    на основе конфигурации (`config.json`).

    Args:
        config (Config): словарь, содержащий параметры конфигурации для процесса скачивания.
    """

    toxicity_config: Config = config["PubChem_download_toxicity"]

    res_folder_name: str = toxicity_config["results_folder_name"]
    res_file_name: str = toxicity_config["results_file_name"]

    if config["testing_flag"]:
        toxicity_config["start_page"] = toxicity_config["end_page"] = 1

    UpdateLoggerFormat(toxicity_config["logger_label"], toxicity_config["logger_color"])

    logger.info(f"{'-' * 21} PubChem downloading for DrugDesign {'-' * 20}")

    for page in range(toxicity_config["start_page"],
                      toxicity_config["end_page"] + 1):
        logger.info(f"Downloading page_{page}...")

        page_folder_name = f"{res_folder_name}/page_{page}"

        os.makedirs(page_folder_name, exist_ok=True)

        compound_link =\
            "https://pubchem.ncbi.nlm.nih.gov/rest/pug_view/annotations/heading/JSON"\
            "?heading=Acute+Effects"\
            f"&page={page}"

        data = GetResponse(compound_link,
                           False,
                           toxicity_config["sleep_time"]).json()["Annotations"]

        annotation_len = len(data["Annotation"])
        if config["verbose_print"]:
            logger.info(f"Amount: {annotation_len}")

        quarters: dict[int, int] = {annotation_len - 1: 100,
                                    int(0.75 * annotation_len): 75,
                                    int(0.50 * annotation_len): 50,
                                    int(0.25 * annotation_len): 25}

        total_pages = int(data["TotalPages"])
        if page > total_pages:
            LogException(IndexError(
                f"Invalid page index: '{page}'! Should be: 1 < 'page' < {total_pages}"))
            continue

        for i, compound_data in enumerate(data["Annotation"]):
            start_time = time.time()

            DownloadCompoundToxicity(compound_data,
                                     page_folder_name,
                                     config)

            end_time = time.time()

            if config["testing_flag"] and config["verbose_print"]:
                logger.info(f"Curr compound: {i}, time: {(end_time - start_time):.3f} sec.")

            if i in quarters.keys() and toxicity_config["need_combining"]:
                quarter = quarters[i]

                logger.info(f"Quarter: {quarter}%, combining files in page_{page} folder...")

                CombineCSVInFolder(page_folder_name,
                                   f"{res_file_name}_{quarters[i]}_page_{page}",
                                   config)

                logger.success(f"Quarter: {quarter}%, combining files in page_{page} folder!")

                # перемещаем, чтобы не конкатенировать его с остальными
                if config["verbose_print"]:
                    logger.info(f"Moving {res_file_name}_"
                                f"{quarters[i]}_page_{page}.csv to "
                                f"{res_folder_name}...")

                MoveFileToFolder(f"{res_file_name}_"
                                 f"{quarters[i]}_page_{page}.csv",
                                 page_folder_name,
                                 res_folder_name)

                if config["verbose_print"]:
                    logger.success(f"Moving {res_file_name}"
                                   f"_{quarters[i]}_page_{page}.csv to "
                                   f"{res_folder_name}!")

                # и удаляем предыдущий
                prev_quarter = quarter - 25

                if prev_quarter != 0:
                    old_quarter_file_name: str =\
                        f"{res_file_name}_{prev_quarter}_page_{page}"

                    if config["verbose_print"]:
                        logger.info("Deleting old quarter file...")

                    os.remove(os.path.join(
                        res_folder_name,
                        f"{old_quarter_file_name}.csv"))

                    if config["verbose_print"]:
                        logger.success("Deleting old quarter file!")

    if toxicity_config["need_combining"]:
        CombineCSVInFolder(res_folder_name,
                           toxicity_config["combined_file_name"],
                           config)

    if toxicity_config["delete_after_combining"] and toxicity_config["need_combining"]:
        if config["verbose_print"]:
            logger.info("Deleting files after combining in "
                        f"'{res_folder_name}'...")

        DeleteFilesInFolder(res_folder_name,
                            [f"{toxicity_config["combined_file_name"]}.csv"],
                            delete_folders=True)

        if config["verbose_print"]:
            logger.success("Deleting files after combining in "
                           f"'{res_folder_name}'!")

    logger.success(f"{'-' * 21} PubChem downloading for DrugDesign {'-' * 20}")
    logger.info(f"{'-' * 77}")
