# from icecream import ic

from PubChem_download_toxicity.functions import *

# ic.disable()

results_folder_name: str = "results/pubchem"
sleep_time: float = 0.3
logger_label: str = "PubChem_toxcomp"  # toxcomp = toxicity_compound
label_color: str = "fg #AEBA66"


def DownloadPubChemCompoundsToxicity(start_page: int = 1, end_page: int = 113,
                                     skip_downloaded_files: bool = False,
                                     print_to_console_verbosely: bool = False) -> None:
    """
    Скачивает данные о токсичности соединений из PubChem, начиная с указанной страницы и заканчивая указанной страницей.

    Args:
        start_page (int, optional): номер начальной страницы для скачивания. Defaults to 1.
        end_page (int, optional): номер конечной страницы для скачивания (включительно). Defaults to 113.
        skip_downloaded_files (bool, optional): если True, то уже скачанные файлы пропускаются. Defaults to False.
        print_to_console_verbosely (bool, optional): если True, то в консоль выводится подробная информация о процессе скачивания. Defaults to False.
    """

    UpdateLoggerFormat(logger_label, label_color)

    logger.info(f"{'-' * 21} PubChem downloading for DrugDesign {'-' * 20}")

    for page in range(start_page, end_page):
        page_dir = f"{results_folder_name}/page_{page}"

        CreateFolder(page_dir)

        compound_link = "https://pubchem.ncbi.nlm.nih.gov/rest/pug_view/annotations/heading/JSON"\
            "?heading=Acute+Effects"\
            "&page={page}"

        try:
            data = GetResponse(compound_link.format(page=page)).json()["Annotations"]

        except Exception as exception:
            PrintException(exception, logger_label, label_color)
            return

        logger.info(f"Amount: {len(data["Annotation"])}".ljust(77))

        total_pages = int(data["TotalPages"])
        if page > total_pages:
            ex = IndexError(f"Invalid page index! Should be: 1 < i < {total_pages}")

            PrintException(ex, logger_label, label_color)
            continue

        for _, compound_data in enumerate(data["Annotation"]):
            DownloadCompoundToxicity(compound_data, page_dir,
                                     logger_label=logger_label,
                                     label_color=label_color,
                                     skip_downloaded_files=skip_downloaded_files,
                                     print_to_console_verbosely=print_to_console_verbosely)

        logger.info(f"{'-' * 77}")
