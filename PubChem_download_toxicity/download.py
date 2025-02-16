# from icecream import ic

from PubChem_download_toxicity.functions import *

# ic.disable()


def DownloadPubChemCompoundsToxicity(config: dict):
    """
    Скачивает информацию о токсичности соединений из базы данных PubChem на основе конфигурации (`config.json`).

    Args:
        config (dict): словарь, содержащий параметры конфигурации для процесса скачивания.
    """

    toxicity = config["PubChem_download_toxicity"]

    if config["testing_flag"]:
        toxicity["start_page"] = toxicity["end_page"] = 1

    UpdateLoggerFormat(toxicity["logger_label"], toxicity["logger_color"])

    logger.info(f"{'-' * 21} PubChem downloading for DrugDesign {'-' * 20}")

    for page in range(toxicity["start_page"], toxicity["end_page"] + 1):
        page_dir = f"{toxicity["results_folder_name"]}/page_{page}"

        CreateFolder(page_dir)

        compound_link = "https://pubchem.ncbi.nlm.nih.gov/rest/pug_view/annotations/heading/JSON"\
            "?heading=Acute+Effects"\
            f"&page={page}"

        try:
            data = GetResponse(compound_link, False,  toxicity["sleep_time"]).json()["Annotations"]

        except Exception as exception:
            LogException(exception)
            return

        logger.info(f"Amount: {len(data["Annotation"])}".ljust(77))

        total_pages = int(data["TotalPages"])
        if page > total_pages:
            LogException(IndexError(f"Invalid page index! Should be: 1 < i < {total_pages}"))
            continue

        for _, compound_data in enumerate(data["Annotation"]):
            DownloadCompoundToxicity(compound_data,
                                     page_dir,
                                     sleep_time=toxicity["sleep_time"],
                                     skip_downloaded_files=config["skip_downloaded"],
                                     print_to_console_verbosely=config["print_to_console_verbosely"],
                                     limit=toxicity["limit"])

        logger.info(f"{'-' * 77}")
