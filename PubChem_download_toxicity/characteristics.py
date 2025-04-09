"""
PubChem_download_toxicity/characteristics.py

Этот модуль содержит функции для фильтрации данных о токсичности соединений
из PubChem по характеристикам и сохранения их в CSV и SDF файлы.
"""

from PubChem_download_toxicity.functions import *

from Utils.dataframe_funcs import MedianDedupedDF


def GetMolfilesFromCIDs(cids: list[str],
                        sleep_time: float | None =
                        toxicity_config["sleep_time"]) -> list[str]:
  """
  Возвращает список molfile-строк для заданного списка CID.
  Соединяет CID в строку, разделяет ее на более короткие подстроки, чтобы избежать
  ограничений на длину URL при запросе к PubChem, и получает molfile для каждого CID.

  Args:
      cids (list[str]): список CID соединений.
      sleep_time (float | None, optional): время ожидания перед повторной попыткой
      в секундах. Defaults to toxicity_config["sleep_time"].

  Returns:
      list[str]: список molfile-строк.
  """

  cids_str = ",".join(str(cid) for cid in cids).replace(" ", "")

  def SplitLongStringWithCommas(s: str) -> list[str]:
    """
    Разбивает длинную строку, содержащую CID, разделенные запятыми,
    на список более коротких строк.

    Это необходимо для избежания ограничений на длину URL при запросе к PubChem.
    Разбивает так, чтобы длина каждой подстроки не превышала 2000 символов.

    Args:
        s (str): строка, содержащая CID, разделенные запятыми.

    Returns:
        list[str]: список строк, содержащих CID, разделенные запятыми.
    """

    if len(s) <= 2000:
      return [s]

    chunks = []
    curr_chunk = ""
    for cid in s.split(","):
      if len(curr_chunk) + len(f"{cid},") <= 2000:
        curr_chunk += f"{cid},"

      else:
        chunks.append(curr_chunk.rstrip(","))
        curr_chunk = f"{cid},"

    if curr_chunk not in chunks:
      chunks.append(curr_chunk.rstrip(","))
    return chunks

  # получаем molfile для каждой подстроки CID.
  molfiles_str: str = ""
  for cids_str_shorter in SplitLongStringWithCommas(cids_str):
    molfiles_str += GetResponse(
        "https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/CID/"
        f"{cids_str_shorter}/record/SDF?record_type=2d",
        True,
        sleep_time).text

  # разделяем строку с molfile на отдельные molfile и очищаем их.
  return [f"\n{molfile.split("\n", 1)[1]}"
          for molfile in molfiles_str.split("\n\n$$$$\n")[:-1]]


def FilterDownloadedToxicityByCharacteristics(unit_type: str,
                                              charact_1: str,
                                              charact_2: str,
                                              charact_3: str,
                                              charact_4: str | None = None) -> None:
  """
  Фильтрует данные о токсичности из CSV-файла по заданным характеристикам,
  загружает molfile для каждого соединения и сохраняет результаты в CSV и SDF файлы.

  Args:
      unit_type (str): тип единиц измерения (например, "kg" или "m3").
      charact_1 (str): название первой характеристики для фильтрации.
      charact_2 (str): название второй характеристики для фильтрации.
      charact_3 (str): название третьей характеристики для фильтрации.
      charact_4 (str | None): название четвёртой (опциональной) характеристики
                              для фильтрации. Defaults to None.
  """

  v_logger.info(f"Filtering by characteristics for {unit_type}...")

  # путь к папке для хранения результатов фильтрации.
  charact_folder_name: str =\
      f"{toxicity_config['results_folder_name']}/"\
      f"{filtering_config['characteristics_folder_name']}"
  os.makedirs(charact_folder_name, exist_ok=True)

  unit_type_df: pd.DataFrame

  try:
    # читаем объединённый CSV-файл, содержащий данные по токсичности для заданного unit_type.
    unit_type_df = pd.read_csv(f"{toxicity_config['results_folder_name']}/"
                               f"{toxicity_config['combined_file_name']}_{unit_type}.csv",
                               sep=config["csv_separator"],
                               low_memory=False)

  except pd.errors.EmptyDataError:
    v_logger.warning(
      f"{unit_type} .csv file is empty, skip filtering by characteristics.")
    return

  # если одна из характеристик - период времени,
  # заменяем отсутствующие значения на "no_exact_time".
  for charact in [charact_1, charact_2, charact_3, charact_4]:
    if charact not in unit_type_df.keys():
      unit_type_df[charact] = np.nan

    if charact == "time_period":
      unit_type_df[charact] = unit_type_df[charact].replace(np.nan, "no_exact_time")

  # уникальные значения для каждой из характеристик.
  unique_charact_1 = unit_type_df[charact_1].unique()
  unique_charact_2 = unit_type_df[charact_2].unique()
  unique_charact_3 = unit_type_df[charact_3].unique()

  # если charact_4 передан, получаем уникальные значения, иначе используем список из одного None.
  unique_charact_4 = unit_type_df[charact_4].unique() if charact_4 else [None]

  v_logger.info(f"Unique {charact_1}s: {unique_charact_1}.", LogMode.VERBOSELY)
  v_logger.info(f"Unique {charact_2}s: {unique_charact_2}.", LogMode.VERBOSELY)
  v_logger.info(f"Unique {charact_3}s: {unique_charact_3}.", LogMode.VERBOSELY)

  if charact_4:
    v_logger.info(f"Unique {charact_4}s: {unique_charact_4}.", LogMode.VERBOSELY)

  # итерация по всем возможным комбинациям характеристик.
  for u_charact_1 in unique_charact_1:
    v_logger.info("-", LogMode.VERBOSELY)
    v_logger.info(f"Current {charact_1}: {u_charact_1}.", LogMode.VERBOSELY)

    for u_charact_2 in unique_charact_2:
      v_logger.info(f"Current {charact_2}: {u_charact_2}.", LogMode.VERBOSELY)

      # фильтрация по первой и второй характеристикам.
      df_lvl2: pd.DataFrame = unit_type_df[
          (unit_type_df[charact_1] == u_charact_1) &
          (unit_type_df[charact_2] == u_charact_2)
      ]

      for u_charact_3 in unique_charact_3:
        # дополнительная фильтрация по третьей характеристике.
        df_lvl3: pd.DataFrame = df_lvl2[df_lvl2[charact_3] == u_charact_3]

        for u_charact_4 in unique_charact_4:
          # финальная фильтрация по четвёртой характеристике, если она указана.
          df_lvl4: pd.DataFrame = df_lvl3 if charact_4 is None else df_lvl3[
            df_lvl3[charact_4] == u_charact_4]

          if df_lvl4.empty:
            continue

          # устраняем дубликаты по CID, усредняя значения дозы.
          df_lvl4 = MedianDedupedDF(df_lvl4, "cid", "dose")

          # если таблица непустая и содержит достаточно записей — продолжаем.
          if len(df_lvl4) >= filtering_config["occurrence_characteristics_number"] and\
                  not df_lvl4.empty:
            df_lvl4["pLD"] = -np.log10((df_lvl4["dose"] / df_lvl4["mw"]) / 1_000_000)

            os.makedirs(f"{charact_folder_name}/{unit_type}", exist_ok=True)

            file_suffix = f"{u_charact_1}_{u_charact_2}_{u_charact_3}"
            if charact_4:
              file_suffix += f"_{u_charact_4}"

            filtered_file_name = f"{charact_folder_name}/{unit_type}/"\
                f"{toxicity_config['results_file_name']}_{file_suffix}"

            if os.path.exists(f"{filtered_file_name}.csv") and config["skip_downloaded"]:
              v_logger.info(f"{file_suffix} is already downloaded, skip.",
                            LogMode.VERBOSELY)
              v_logger.info("~", LogMode.VERBOSELY)

              continue

            # сохраняем отфильтрованный DataFrame в CSV.
            df_lvl4.to_csv(f"{filtered_file_name}.csv", index=False)

            # если необходимо сохранить структуру соединений в формате SDF.
            if toxicity_config["download_compounds_sdf"]:
              v_logger.info(f"Saving {unit_type} characteristics to .sdf...",
                            LogMode.VERBOSELY)

              # получаем список CID'ов и скачиваем molfile.
              cids: list[str] = list(df_lvl4["cid"])
              SaveMolfilesToSDF(
                  data=pd.DataFrame({"cid": cids,
                                     "molfile": GetMolfilesFromCIDs(cids)}),
                  file_name=filtered_file_name,
                  molecule_id_column_name="cid",
                  extra_data=df_lvl4,
                  indexing_lists=True
              )

              v_logger.success(f"Saving {unit_type} characteristics to .sdf!",
                               LogMode.VERBOSELY)

            # логируем успешное сохранение данных.
            v_logger.success(
              f"Saved {file_suffix}, len: {len(df_lvl4)}!", LogMode.VERBOSELY)
            v_logger.info("~", LogMode.VERBOSELY)

  # логируем завершение процесса фильтрации.
  v_logger.success(f"Filtering by characteristics for {unit_type}!")
