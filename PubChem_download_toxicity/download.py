# from icecream import ic

from PubChem_download_toxicity.functions import *

# ic.disable()


@ReTry(attempts_amount=1)
def DownloadPubChemCompoundsToxicity(config: dict):
    """
    Скачивает информацию о токсичности соединений из базы данных PubChem 
    на основе конфигурации (`config.json`).

    Args:
        config (dict): словарь, содержащий параметры конфигурации для процесса скачивания.
    """

    toxicity_config = config["PubChem_download_toxicity"]
    res_folder_name: str = toxicity_config["results_folder_name"]
    res_file_name: str = toxicity_config["results_file_name"]

    if config["testing_flag"]:
        toxicity_config["start_page"] = toxicity_config["end_page"] = 1

    UpdateLoggerFormat(toxicity_config["logger_label"], toxicity_config["logger_color"])

    logger.info(f"{'-' * 21} PubChem downloading for DrugDesign {'-' * 20}")

    for page in range(toxicity_config["start_page"],
                      toxicity_config["end_page"] + 1):
        page_folder_name = f"{res_folder_name}/page_{page}"

        CreateFolder(page_folder_name)

        compound_link = "https://pubchem.ncbi.nlm.nih.gov/rest/pug_view/annotations/heading/JSON"\
            "?heading=Acute+Effects"\
            f"&page={page}"

        data = GetResponse(compound_link, False,  toxicity_config["sleep_time"]).json()[
            "Annotations"]

        annotation_len = len(data["Annotation"])
        logger.info(f"Amount: {annotation_len}")

        quarters: dict[int, int] = {annotation_len - 1: 100,
                                    int(0.75 * annotation_len): 75,
                                    int(0.50 * annotation_len): 50,
                                    int(0.25 * annotation_len): 25}

        total_pages = int(data["TotalPages"])
        if page > total_pages:
            LogException(IndexError(f"Invalid page index! Should be: 1 < i < {total_pages}"))
            continue

        for i, compound_data in enumerate(data["Annotation"]):
            start_time = time.time()

            DownloadCompoundToxicity(compound_data,
                                     page_folder_name,
                                     sleep_time=toxicity_config["sleep_time"],
                                     skip_downloaded_files=config["skip_downloaded"],
                                     print_to_console_verbosely=config["print_to_console_verbosely"],
                                     limit=toxicity_config["limit"])

            end_time = time.time()

            if config["testing_flag"]:
                logger.info(f"Counter: {i}, time: {(end_time - start_time):.3f}.")

            if i in quarters.keys() and toxicity_config["need_combining"]:
                CombineCSVInFolder(page_folder_name,
                                   f"{res_file_name}_{quarters[i]}_page_{page}",
                                   print_to_console=config["print_to_console_verbosely"],
                                   skip_downloaded_files=config["skip_downloaded"])

                # перемещаем, чтобы не конкатенировать его с остальными
                if config["print_to_console_verbosely"]:
                    logger.info(f"Moving {res_file_name}_"
                                f"{quarters[i]}_page_{page}.csv to "
                                f"{res_folder_name}...")

                MoveFileToFolder(f"{res_file_name}_"
                                 f"{quarters[i]}_page_{page}.csv",
                                 page_folder_name,
                                 res_folder_name)

                if config["print_to_console_verbosely"]:
                    logger.success(f"Moving {res_file_name}"
                                   f"_{quarters[i]}_page_{page}.csv to "
                                   f"{res_folder_name}!")

                # и удаляем предыдущий
                quarter = quarters[i]
                prev_quarter = quarter - 25

                if prev_quarter != 0:
                    old_quarter_file_name: str =\
                        f"{res_file_name}_{prev_quarter}_page_{page}"

                    if config["print_to_console_verbosely"]:
                        logger.info("Deleting old quarter file...")

                    os.remove(os.path.join(
                        res_folder_name,
                        f"{old_quarter_file_name}.csv"))

                    if config["print_to_console_verbosely"]:
                        logger.success("Deleting old quarter file!")

    if toxicity_config["need_combining"]:
        CombineCSVInFolder(res_folder_name,
                           toxicity_config["combined_file_name"],
                           print_to_console=config["print_to_console_verbosely"],
                           skip_downloaded_files=config["skip_downloaded"])

    if toxicity_config["delete_after_combining"] and toxicity_config["need_combining"]:
        if config["print_to_console_verbosely"]:
            logger.info("Deleting files after combining in "
                        f"'{res_folder_name}'...")

        DeleteFilesInFolder(res_folder_name,
                            [f"{toxicity_config["combined_file_name"]}.csv"],
                            delete_folders=True)

        if config["print_to_console_verbosely"]:
            logger.success("Deleting files after combining in "
                           f"'{res_folder_name}'!")

    logger.success(f"{'-' * 21} PubChem downloading for DrugDesign {'-' * 20}")
    logger.info(f"{'-' * 77}")
