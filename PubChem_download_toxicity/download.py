from PubChem_download_toxicity.functions import *

from Utils.files_funcs import CombineCSVInFolder, DeleteFilesInFolder, \
    MoveFileToFolder, os
from Utils.verbose_logger import v_logger, LogMode

from Configurations.config import config, Config


@ReTry(attempts_amount=1)
def DownloadPubChemCompoundsToxicity():
    """
    Скачивает информацию о токсичности соединений из базы данных PubChem 
    на основе конфигурации (`config.json`).
    """

    toxicity_config: Config = config["PubChem_download_toxicity"]
    results_folder_kg: str = f"{toxicity_config["results_folder_name"]}/kg"
    results_folder_m3: str = f"{toxicity_config["results_folder_name"]}/m3"

    if config["testing_flag"]:
        toxicity_config["start_page"] = 1
        toxicity_config["end_page"] = 1

    v_logger.UpdateFormat(toxicity_config["logger_label"],
                          toxicity_config["logger_color"])

    v_logger.info(f"{'-' * 21} PubChem downloading for DrugDesign {'-' * 20}")

    for page in range(toxicity_config["start_page"],
                      toxicity_config["end_page"] + 1):
        v_logger.info(f"Downloading page_{page}...")

        page_folder_name = f"{toxicity_config["results_folder_name"]}/""{unit_type}"\
            f"/page_{page}"

        os.makedirs(page_folder_name.format(unit_type="kg"), exist_ok=True)
        os.makedirs(page_folder_name.format(unit_type="m3"), exist_ok=True)

        compound_link =\
            "https://pubchem.ncbi.nlm.nih.gov/rest/pug_view/annotations/heading/JSON"\
            "?heading=Acute+Effects"\
            f"&page={page}"

        data = GetResponse(compound_link,
                           False,
                           toxicity_config["sleep_time"]).json()["Annotations"]

        annotation_len = len(data["Annotation"])
        v_logger.info(f"Amount: {annotation_len}", LogMode.VERBOSELY)

        quarters: dict[int, int] = {annotation_len - 1: 100,
                                    int(0.75 * annotation_len): 75,
                                    int(0.50 * annotation_len): 50,
                                    int(0.25 * annotation_len): 25}

        total_pages = int(data["TotalPages"])
        if page > total_pages:
            v_logger.LogException(IndexError(
                f"Invalid page index: '{page}'! Should be: 1 < 'page' < {total_pages}"))
            continue

        for i, compound_data in enumerate(data["Annotation"]):
            start_time = time.time()

            DownloadCompoundToxicity(compound_data,
                                     page_folder_name)

            end_time = time.time()

            if config["testing_flag"]:
                v_logger.info(f"Prev compound: {i}, time: {(end_time - start_time):.3f} sec.",
                              LogMode.VERBOSELY)

            if i in quarters.keys() and toxicity_config["need_combining"]:
                quarter = quarters[i]

                v_logger.info(f"Quarter: {quarter}%, combining files in page_{page} folder...")

                CombineCSVInFolder(page_folder_name.format(unit_type="kg"),
                                   f"{toxicity_config["results_file_name"]}_{quarters[i]}"
                                   f"_page_{page}")

                CombineCSVInFolder(page_folder_name.format(unit_type="m3"),
                                   f"{toxicity_config["results_file_name"]}_{quarters[i]}"
                                   f"_page_{page}")

                v_logger.success(f"Quarter: {quarter}%, "
                                 f"combining files in page_{page} folder!")

                # перемещаем, чтобы не конкатенировать его с остальными
                v_logger.info(f"Moving {toxicity_config["results_file_name"]}_"
                              f"{quarters[i]}_page_{page}.csv to "
                              f"{toxicity_config["results_folder_name"]}...",
                              LogMode.VERBOSELY)

                quarter_file_name = f"{toxicity_config["results_file_name"]}_"\
                    f"{quarters[i]}_page_{page}.csv"

                MoveFileToFolder(quarter_file_name,
                                 page_folder_name.format(unit_type="kg"),
                                 results_folder_kg)

                MoveFileToFolder(quarter_file_name,
                                 page_folder_name.format(unit_type="m3"),
                                 results_folder_m3)

                v_logger.success(f"Moving {quarter_file_name} to "
                                 f"{toxicity_config["results_folder_name"]}!",
                                 LogMode.VERBOSELY)

                # и удаляем предыдущий
                prev_quarter = quarter - 25

                if prev_quarter != 0:
                    old_quarter_file_name: str =\
                        f"{toxicity_config["results_file_name"]}_{prev_quarter}_page_{page}"

                    v_logger.info("Deleting old quarter file...",
                                  LogMode.VERBOSELY)

                    if os.path.exists(os.path.join(results_folder_kg,
                                                   f"{old_quarter_file_name}.csv")):
                        os.remove(os.path.join(
                            results_folder_kg,
                            f"{old_quarter_file_name}.csv"))

                    if os.path.exists(os.path.join(results_folder_m3,
                                                   f"{old_quarter_file_name}.csv")):
                        os.remove(os.path.join(
                            results_folder_m3,
                            f"{old_quarter_file_name}.csv"))

                    v_logger.success("Deleting old quarter file!",
                                     LogMode.VERBOSELY)

    if toxicity_config["need_combining"]:
        CombineCSVInFolder(results_folder_kg,
                           f"{toxicity_config["combined_file_name"]}_kg")

        MoveFileToFolder(f"{toxicity_config["combined_file_name"]}_kg.csv",
                         results_folder_kg,
                         toxicity_config["results_folder_name"])

        CombineCSVInFolder(results_folder_m3,
                           f"{toxicity_config["combined_file_name"]}_m3")

        MoveFileToFolder(f"{toxicity_config["combined_file_name"]}_m3.csv",
                         results_folder_m3,
                         toxicity_config["results_folder_name"])

    if toxicity_config["delete_after_combining"] and toxicity_config["need_combining"]:
        v_logger.info("Deleting files after combining in "
                      f"'{toxicity_config["results_folder_name"]}'...",
                      LogMode.VERBOSELY)

        except_items: list[str] = [f"{toxicity_config["combined_file_name"]}_kg.csv",
                                   f"{toxicity_config["combined_file_name"]}_m3.csv"]
        molfiles_folder_name: str = toxicity_config["molfiles_folder_name"]

        # в том случае, если molfiles_folder_name находится внутри results_folder_name
        # (т.е. путь к molfiles_folder_name содержит путь к results_folder_name)
        if toxicity_config["results_folder_name"] in molfiles_folder_name:
            except_items.append(molfiles_folder_name.replace(
                toxicity_config["results_folder_name"], "").split("/")[1])

        DeleteFilesInFolder(toxicity_config["results_folder_name"],
                            except_items,
                            delete_folders=True)

        v_logger.success("Deleting files after combining in "
                         f"'{toxicity_config["results_folder_name"]}'!", LogMode.VERBOSELY)

    v_logger.success(f"{'-' * 21} PubChem downloading for DrugDesign {'-' * 20}")
    v_logger.info(f"{'-' * 77}")
