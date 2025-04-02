"""
PubChem_download_toxicity/download.py

Этот модуль отвечает за скачивание данных о токсичности соединений из базы
данных PubChem и их обработку.
"""

from PubChem_download_toxicity.functions import *

from Utils.files_funcs import CombineCSVInFolder, DeleteFilesInFolder, \
    IsFileInFolder, MoveFileToFolder, os
from Utils.verbose_logger import v_logger, LogMode

from Configurations.config import config, Config


@ReTry(attempts_amount=1)
def DownloadPubChemCompoundsToxicity():
  """
  Скачиваем информацию о токсичности соединений из базы данных PubChem на
  основе конфигурации (`config.json`).
  """

  # конфигурация для скачивания данных о токсичности.
  toxicity_config: Config = config["PubChem_download_toxicity"]

  # путь к папке для результатов в единицах "kg".
  results_folder_kg: str = f"{toxicity_config["results_folder_name"]}/kg"
  # путь к папке для результатов в единицах "m3".
  results_folder_m3: str = f"{toxicity_config["results_folder_name"]}/m3"

  # если установлен флаг тестирования, ограничиваем диапазон страниц.
  if config["testing_flag"]:
    toxicity_config["start_page"] = 1
    toxicity_config["end_page"] = 3

  v_logger.UpdateFormat(toxicity_config["logger_label"],
                        toxicity_config["logger_color"])

  v_logger.info(f"{"• " * 10} PubChem downloading for DrugDesign.")

  # если файлы не скачаны или их нет в папке.
  if not config["skip_downloaded"] or\
      not IsFileInFolder(
          f"{toxicity_config["combined_file_name"]}_m3.csv",
          toxicity_config["results_folder_name"]) or\
      not IsFileInFolder(
          f"{toxicity_config["combined_file_name"]}_kg.csv",
          toxicity_config["results_folder_name"]):
    # итерируемся по страницам (включая последнюю).
    for page_num in range(toxicity_config["start_page"],
                          toxicity_config["end_page"] + 1):
      v_logger.info(f"Downloading page_{page_num}...")

      # формируем имя папки для текущей страницы.
      page_folder_name = f"{toxicity_config["results_folder_name"]}/"\
          "{unit_type}/page_{page_num}"

      # если существуют папки для следующих страниц, значит, эти полностью загружены.
      if config["skip_downloaded"] and\
              (os.path.exists(page_folder_name.format(unit_type="kg",
                                                      page_num=page_num + 1)) or
               os.path.exists(page_folder_name.format(unit_type="m3",
                                                      page_num=page_num + 1))):
        v_logger.info(f"Folder for page_{page_num} is already exists, skip.")
        continue

      # создаем директории для единиц измерения "kg" и "m3".
      os.makedirs(page_folder_name.format(unit_type="kg",
                                          page_num=page_num), exist_ok=True)
      os.makedirs(page_folder_name.format(unit_type="m3",
                                          page_num=page_num), exist_ok=True)

      # формируем ссылку для скачивания данных о соединениях.
      compound_link: str =\
          "https://pubchem.ncbi.nlm.nih.gov/rest/pug_view/annotations/"\
          "heading/JSON"\
          "?heading=Acute+Effects"\
          f"&page={page_num}"

      # получаем данные с веб-страницы.
      data = GetResponse(compound_link,
                         False,
                         toxicity_config["sleep_time"]).json()["Annotations"]

      # получаем количество аннотаций на странице.
      annotation_len = len(data["Annotation"])
      v_logger.info(f"Amount: {annotation_len}", LogMode.VERBOSELY)

      # определяем границы для объединения файлов по частям.
      quarters: dict[int, int] = {annotation_len - 1: 100,
                                  int(0.75 * annotation_len): 75,
                                  int(0.50 * annotation_len): 50,
                                  int(0.25 * annotation_len): 25}

      # получаем общее количество страниц.
      total_pages = int(data["TotalPages"])

      # проверяем, что номер текущей страницы не превышает общее количество.
      if page_num > total_pages:
        v_logger.LogException(IndexError(
            f"Invalid page index: '{page_num}'! Should be: 1 < 'page' < "
            f"{total_pages}"))
        continue

      # итерируемся по данным о соединениях.
      for i, compound_data in enumerate(data["Annotation"]):
        # фиксируем время начала обработки.
        start_time = time.time()

        # скачиваем данные о токсичности соединения.
        DownloadCompoundToxicity(compound_data,
                                 f"{toxicity_config["results_folder_name"]}/"
                                 "{unit_type}/"f"page_{page_num}")

        # фиксируем время окончания обработки.
        end_time = time.time()

        # если включен флаг тестирования, выводим время обработки.
        if config["testing_flag"]:
          v_logger.info(
              f"Prev compound: {i}, time: {(end_time - start_time):.3f}"
              f" sec.",
              LogMode.VERBOSELY)

        # если достигнута граница для объединения файлов.
        if i in quarters.keys() and toxicity_config["need_combining"]:
          # получаем номер текущей границы.
          quarter = quarters[i]

          v_logger.info(f"Quarter: {quarter}%, combining files in "
                        f"page_{page_num} folder...")

          # объединяем CSV-файлы для единиц измерения "kg".
          CombineCSVInFolder(page_folder_name.format(unit_type="kg",
                                                     page_num=page_num),
                             f"{toxicity_config["results_file_name"]}_"
                             f"{quarters[i]}_page_{page_num}")

          # объединяем CSV-файлы для единиц измерения "m3".
          CombineCSVInFolder(page_folder_name.format(unit_type="m3",
                                                     page_num=page_num),
                             f"{toxicity_config["results_file_name"]}_"
                             f"{quarters[i]}_page_{page_num}")

          v_logger.success(f"Quarter: {quarter}%, combining files in "
                           f"page_{page_num} folder!")

          # перемещаем объединенные файлы.
          v_logger.info(
              f"Moving {toxicity_config["results_file_name"]}_"
              f"{quarters[i]}_page_{page_num}.csv to "
              f"{toxicity_config["results_folder_name"]}...",
              LogMode.VERBOSELY)

          # формируем имя файла для перемещения.
          quarter_file_name = f"{toxicity_config["results_file_name"]}_"\
              f"{quarters[i]}_page_{page_num}.csv"

          # перемещаем файл для единиц измерения "kg".
          MoveFileToFolder(quarter_file_name,
                           page_folder_name.format(unit_type="kg",
                                                   page_num=page_num),
                           results_folder_kg)

          # перемещаем файл для единиц измерения "m3".
          MoveFileToFolder(quarter_file_name,
                           page_folder_name.format(unit_type="m3",
                                                   page_num=page_num),
                           results_folder_m3)

          v_logger.success(
              f"Moving {quarter_file_name} to "
              f"{toxicity_config["results_folder_name"]}!",
              LogMode.VERBOSELY)

          # удаляем предыдущий объединенный файл.
          prev_quarter = quarter - 25

          # если предыдущая часть не равна нулю.
          if prev_quarter != 0:
            # формируем имя предыдущего файла.
            old_quarter_file_name: str =\
                f"{toxicity_config["results_file_name"]}_"\
                f"{prev_quarter}_page_{page_num}"

            v_logger.info("Deleting old quarter file...",
                          LogMode.VERBOSELY)

            # удаляем старый файл для единиц измерения "kg".
            if os.path.exists(os.path.join(results_folder_kg,
                                           f"{old_quarter_file_name}.csv")):
              os.remove(os.path.join(
                  results_folder_kg,
                  f"{old_quarter_file_name}.csv"))

            # удаляем старый файл для единиц измерения "m3".
            if os.path.exists(os.path.join(results_folder_m3,
                                           f"{old_quarter_file_name}.csv")):
              os.remove(os.path.join(
                  results_folder_m3,
                  f"{old_quarter_file_name}.csv"))

            v_logger.success("Deleting old quarter file!",
                             LogMode.VERBOSELY)

    if toxicity_config["need_combining"]:
      # объединяем все CSV-файлы в папке для единиц измерения "kg".
      CombineCSVInFolder(results_folder_kg,
                         f"{toxicity_config["combined_file_name"]}_kg")

      # перемещаем объединенный файл в основную папку.
      MoveFileToFolder(f"{toxicity_config["combined_file_name"]}_kg.csv",
                       results_folder_kg,
                       toxicity_config["results_folder_name"])

      # объединяем все CSV-файлы в папке для единиц измерения "m3".
      CombineCSVInFolder(results_folder_m3,
                         f"{toxicity_config["combined_file_name"]}_m3")

      # перемещаем объединенный файл в основную папку.
      MoveFileToFolder(f"{toxicity_config["combined_file_name"]}_m3.csv",
                       results_folder_m3,
                       toxicity_config["results_folder_name"])

    # если включено удаление файлов после объединения и объединение включено.
    if toxicity_config["delete_after_combining"] and \
            toxicity_config["need_combining"]:
      v_logger.info(
          f"Deleting files after combining in "
          f"'{toxicity_config["results_folder_name"]}'...",
          LogMode.VERBOSELY)

      # определяем файлы, которые не нужно удалять.
      except_items: list[str] = [
          f"{toxicity_config["combined_file_name"]}_kg.csv",
          f"{toxicity_config["combined_file_name"]}_m3.csv"]
      # получаем имя папки с molfile.
      molfiles_folder_name: str = toxicity_config["molfiles_folder_name"]

      # проверяем, что molfiles_folder_name находится внутри results_folder_name.
      if toxicity_config["results_folder_name"] in molfiles_folder_name:
        except_items.append(molfiles_folder_name.replace(
            toxicity_config["results_folder_name"], "").split("/")[1])

      # удаляем все файлы, кроме указанных в списке except_items.
      DeleteFilesInFolder(toxicity_config["results_folder_name"],
                          except_items,
                          delete_folders=True)

      v_logger.success(
          f"Deleting files after combining in "
          f"'{toxicity_config["results_folder_name"]}'!",
          LogMode.VERBOSELY)

  # если файлы уже скачаны, пропускаем.
  else:
    v_logger.info(
        f"{toxicity_config["results_file_name"]} is already "
        f"downloaded, skip.",
        LogMode.VERBOSELY)

  if toxicity_config["filtering"]["need_filtering_by_characteristics"]:
    v_logger.info("·", LogMode.VERBOSELY)

    FilterDownloadedToxicityByCharacteristics("m3",
                                              "organism",
                                              "time_period",
                                              "testtype")

    v_logger.info()

    FilterDownloadedToxicityByCharacteristics("kg",
                                              "organism",
                                              "route",
                                              "testtype")

  v_logger.success(f"{"• " * 10} PubChem downloading for DrugDesign!")
  v_logger.info()
