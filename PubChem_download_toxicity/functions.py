"""
PubChem_download_toxicity/functions.py

Этот модуль содержит функции для скачивания данных о токсичности соединений
из PubChem, их фильтрации, преобразования и сохранения в CSV и SDF файлы.
"""

from io import StringIO
import json
import numpy as np
import requests
import urllib.parse

from Utils.dataframe_funcs import DedupedList, MedianDedupedDF
from Utils.decorators import ReTry, time
from Utils.files_funcs import os, pd, SaveMolfilesToSDF
from Utils.verbose_logger import v_logger, LogMode

from Configurations.config import config, Config

# конфигурация для скачивания токсичности.
toxicity_config: Config = config["PubChem_download_toxicity"]

# конфигурация конфигурацию для фильтрации токсичности.
filtering_config: Config = toxicity_config["filtering"]


@ReTry()
def GetResponse(request_url: str,
                stream: bool,
                sleep_time: float | None =
                config["PubChem_download_toxicity"]["sleep_time"]
                ) -> requests.Response:
  """
  Отправляет GET-запрос по указанному URL, повторяет попытку в случае ошибки.

  Args:
      request_url (str): URL для запроса.
      stream (bool): если True, ответ будет получен потоком.
      sleep_time (float | None, optional): время ожидания перед повторной попыткой
      в секундах. Defaults to config["PubChem_download_toxicity"]["sleep_time"].

  Returns:
      requests.Response: объект ответа requests.
  """

  # ждем указанное время, если оно задано.
  if sleep_time is not None:
    time.sleep(sleep_time)

  # отправляем GET-запрос.
  response = requests.get(request_url, stream=stream)
  response.raise_for_status()

  return response


def GetMolfileFromCID(cid: str,
                      sleep_time: float | None =
                      config["PubChem_download_toxicity"]["sleep_time"]
                      ) -> str:
  """
  Возвращает molfile-строку из GET-запроса для соединения с cid из базы PubChem.

  Args:
      cid (str): CID соединения.
      sleep_time (float | None, optional): время ожидания перед повторной попыткой
      в секундах. Defaults to config["PubChem_download_toxicity"]["sleep_time"].

  Returns:
      str: molfile-строка.
  """

  # получаем molfile соединения из PubChem.
  molfile: str = GetResponse(
      "https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/CID/"
      f"{cid}/record/SDF?record_type=2d",
      True,
      sleep_time).text

  v_logger.info(f"Return molfile (len: {len(molfile)}) for cid: {cid}.",
                LogMode.VERBOSELY)

  # очищаем molfile от лишних символов.
  return molfile[molfile.find("\n"):].replace("$$$$", "").rstrip()


def GetMolfilesFromCIDs(cids: list[str],
                        sleep_time: float | None =
                        config["PubChem_download_toxicity"]["sleep_time"]) -> list[str]:
  """
  Возвращает список molfile-строк для заданного списка CID.
  Соединяет CID в строку, разделяет ее на более короткие подстроки, чтобы избежать
  ограничений на длину URL при запросе к PubChem, и получает molfile для каждого CID.

  Args:
      cids (list[str]): список CID соединений.
      sleep_time (float | None, optional): время ожидания перед повторной попыткой
      в секундах. Defaults to config["PubChem_download_toxicity"]["sleep_time"].

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


def GetDataFrameFromUrl(request_url: str, sleep_time: float) -> pd.DataFrame:
  """
  Скачивает данные из CSV-файла по URL и преобразует их в pandas.DataFrame.

  Args:
      request_url (str): URL CSV-файла.
      sleep_time (float): время ожидания перед повторной попыткой в секундах.

  Returns:
      pd.DataFrame: DataFrame, содержащий данные из CSV-файла.
  """

  # получаем ответ на запрос.
  res = GetResponse(request_url, True, sleep_time)

  # определяем кодировку из заголовков ответа.
  if res.encoding is None:
    res.encoding = "utf-8"  # (UTF-8, если кодировка не указана)

  # читаем CSV из ответа и преобразуем в DataFrame.
  return pd.read_csv(StringIO(res.content.decode(res.encoding)))


def GetLinkFromSid(sid: int,
                   collection: str,
                   limit: int
                   ) -> str:
  """
  Формируем URL для скачивания данных из PubChem SDQ API по SID (Structure ID).

  Args:
      sid (int): SID соединения.
      collection (str): коллекция для поиска.
      limit (int): максимальное количество возвращаемых записей.

  Returns:
      str: URL для скачивания данных.
  """

  def QueryDictToStr(query: dict[str, str]) -> str:
    """
    Преобразует словарь с параметрами запроса в строку запроса URL-encoded.

    Args:
        query (dict[str, str]): словарь с параметрами запроса.

    Returns:
        str: строка запроса в формате "query={JSON-encoded query}".
             Пустая строка, если словарь пуст.
    """

    # если словарь пуст, возвращаем пустую строку.
    if not query:
      return ""

    # преобразуем словарь в строку запроса.
    return f"query={urllib.parse.quote(json.dumps(query))}"

  # формируем словарь с параметрами запроса.
  query = {
      "download": "*",
      "collection": f"{collection}",
      # "order": ["relevancescore,desc"],
      "limit": f"{limit}",
      "where": {
          "ands": [
              {"sid": f"{sid}"}
          ]
      }
  }

  # формируем URL для запроса.
  start = "https://pubchem.ncbi.nlm.nih.gov/sdq/sdqagent.cgi"\
      "?infmt=json"\
      "&outfmt=csv"

  return start + "&" + QueryDictToStr(query)


# MARK: DownloadCompoundToxicity


@ReTry(attempts_amount=1)
def DownloadCompoundToxicity(compound_data: dict,
                             page_folder_name: str):
  """
  Скачиваем данные о токсичности соединения по информации из JSON PubChem
  и сохраняем их в CSV-файл.

  Args:
      compound_data (dict): словарь с информацией о соединении из JSON PubChem.
      page_folder_name (str): путь к директории, в которой будет сохранен файл.
  """

  cid: str = ""

  try:
    # пытаемся получить CID соединения.
    cid = compound_data["LinkedRecords"]["CID"][0]

  # если CID отсутствует.
  except KeyError:
    v_logger.warning(
        f"No 'cid' for 'sid': {compound_data["LinkedRecords"]["SID"][0]}"
        f", skip.")
    v_logger.info("-", LogMode.VERBOSELY)

    return
    # не сохраняем те соединения, у которых нет cid,
    # так как невозможно вычислить молекулярные вес

  primary_sid: int | None
  try:
    # пытаемся получить SID соединения.
    primary_sid = int(compound_data["LinkedRecords"]["SID"][0])

  # если SID отсутствует.
  except KeyError:
    primary_sid = None

  # получаем данные из таблицы.
  raw_table: str = compound_data["Data"][0]["Value"]["ExternalTableName"]
  table_info: dict = {}

  # разбираем данные таблицы.
  for row in raw_table.split("&"):
    key, value = row.split("=")
    table_info[key] = value

  # проверяем тип запроса.
  if table_info["query_type"] != "sid":
    v_logger.LogException(ValueError("Unknown query type at page "
                                     f"{page_folder_name}"))

  # получаем SID из данных таблицы.
  sid = int(table_info["query"])

  # проверяем соответствие SID.
  if primary_sid != sid:
    v_logger.warning(f"Mismatch between 'primary_sid' ({primary_sid}) "
                     f"and 'sid' ({sid}).")

  # формируем имя файла.
  compound_name: str = f"compound_{sid}_toxicity"

  # формируем пути к файлам для разных единиц измерения.
  compound_file_kg = f"{page_folder_name.format(unit_type="kg")}/"\
      f"{compound_name}"
  compound_file_m3 = f"{page_folder_name.format(unit_type="m3")}/"\
      f"{compound_name}"

  # если файл уже существует и скачивание пропущено, пропускаем.
  if os.path.exists(f"{compound_file_kg}.csv") or\
          os.path.exists(f"{compound_file_m3}.csv") and\
          config["skip_downloaded"]:
    v_logger.info(f"{compound_name} is already downloaded, skip.",
                  LogMode.VERBOSELY)
    v_logger.info("-", LogMode.VERBOSELY)

    return

  v_logger.info(f"Downloading {compound_name}...", LogMode.VERBOSELY)

  # получаем данные о токсичности из PubChem.
  acute_effects = GetDataFrameFromUrl(
      GetLinkFromSid(sid=sid,
                     collection=table_info["collection"],
                     limit=toxicity_config["limit"]),
      toxicity_config["sleep_time"]
  )

  @ReTry()
  def GetMolecularWeightByCid(cid: str | int) -> str:
    """
    Получает молекулярный вес соединения из PubChem REST API, используя его CID.

    Args:
        cid (str | int): PubChem Compound Identifier (CID) соединения.

    Returns:
        str: молекулярный вес соединения в виде строки.
    """

    # получаем молекулярный вес соединения из PubChem.
    return GetResponse(
        "https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/cid/"
        f"{cid}/property/MolecularWeight/txt",
        True, None).text.strip()

  def CalcMolecularWeight(df: pd.DataFrame,
                          id_column: str,
                          ) -> pd.DataFrame:
    """
    Вычисляет и добавляет столбец 'mw' (молекулярный вес) в pd.DataFrame.

    Args:
        df (pd.DataFrame): исходный pd.DataFrame.
        id_column (str): название столбца, содержащего ID соединений.

    Returns:
        pd.DataFrame: модифицированный DataFrame с добавленным столбцом 'mw'.
    """

    # получаем уникальные идентификаторы соединений.
    unique_ids = df[id_column].dropna().unique()

    # если найден только один уникальный идентификатор.
    if len(unique_ids) == 1:
      # получаем молекулярный вес для этого идентификатора.
      mw = GetMolecularWeightByCid(unique_ids[0])

      # если молекулярный вес найден.
      if mw is not None:
        # добавляем столбец с молекулярным весом в DataFrame.
        df["mw"] = mw

        v_logger.info(f"Found 'mw' by '{id_column}'.",
                      LogMode.VERBOSELY)

      # если молекулярный вес не найден.
      else:
        v_logger.warning(
            "Could not retrieve molecular weight by "
            f"'{id_column}' for {unique_ids[0]}.")

    # если идентификаторы не найдены.
    elif len(unique_ids) == 0:
      v_logger.warning(f"No '{id_column}' found for {unique_ids[0]}.")

    # если идентификаторов несколько.
    else:
      v_logger.warning(
          f"Non-unique 'mw' by {id_column} for {unique_ids[0]}.")

      # применяем функцию получения молекулярного веса к каждому id.
      df["mw"] = df[id_column].apply(GetMolecularWeightByCid)

      # если некоторые значения молекулярного веса не найдены.
      if df["mw"].isnull().any():
        v_logger.warning(
            f"Some 'mw' could not be retrieved by {id_column}.")

    return df

  def ExtractDoseAndTime(df: pd.DataFrame,
                         valid_units: list[str]) -> pd.DataFrame:
    """
    Преобразует DataFrame с данными о дозировках, извлекая числовое
    значение, единицу измерения и период времени.

    Args:
        df (pd.DataFrame): таблица с колонкой "dose", содержащей
                            информацию о дозировках.
        valid_units (list[str]): список допустимых единиц измерения дозы.

    Returns:
        DataFrame с тремя новыми колонками: "numeric_dose", "dose_value",
        "time_period".
    """

    def ExtractDose(dose_str: str,
                    mw: float
                    ) -> tuple[float | None, str | None, str | None]:
      """
      Извлекает дозу, единицу измерения и период времени из строки
      дозировки.

      Args:
          dose_str (str): строка, содержащая информацию о дозировке.
          mw (float): молекулярная масса соединения.

      Returns:
          tuple[float | None, str | None, str | None]: кортеж, содержащий:
              - числовую дозу (float или None, если извлечь не удалось).
              - единицу измерения дозы (str или None, если извлечь не удалось).
              - период времени (str или None, если извлечь не удалось).
      """

      # если в строке нет пробелов, возвращаем None.
      if " " not in dose_str:
        return None, None, None

      num_dose: float | str | None = None
      dose_unit: str | None = None
      time_per: str | None = None

      try:
        # если строка дозировки содержит не два элемента, возвращаем None.
        if len(dose_str.split(" ")) != 2:
          return None, None, None

        # разделяем строку на количество дозы и единицы измерения.
        dose_amount_str, dose_and_time = dose_str.split(" ")
        # преобразуем количество дозы в число.
        num_dose = float(dose_amount_str)

      # если не удалось преобразовать количество дозы в число.
      except ValueError:
        v_logger.warning(f"Unsupported dose string: {dose_str}",
                         LogMode.VERBOSELY)
        return None, None, None

      # определяем, есть ли период времени.
      match dose_str.count("/"):
        case 1:      # нету time period или это pp*/time
          # если строка начинается с "p", это "pp*/time".
          if dose_and_time.startswith("p"):
            dose_unit, time_per = dose_and_time.split("/")
          else:
            dose_unit = dose_and_time
            time_per = None

        case 2:      # есть time period
          # извлекаем единицу измерения и период времени.
          dose_unit = "/".join(dose_and_time.split("/")[:-1])
          time_per = dose_and_time.split("/")[-1]

        case _:
          return None, None, None

      # если единица измерения не поддерживается.
      if dose_unit not in valid_units:
        v_logger.warning(f"Unsupported dose_unit: {dose_unit}",
                         LogMode.VERBOSELY)
        return None, None, None

      unit_prefix: str = dose_unit
      unit_suffix: str = "m3"

      # если единица измерения содержит "/", разделяем ее.
      if dose_unit.count("/") > 0:
        unit_prefix, unit_suffix = dose_unit.split("/")

        # если суффикс не поддерживается.
        if unit_suffix not in ("kg", "m3"):
          v_logger.warning(f"Unsupported dose_unit: {dose_unit}",
                           LogMode.VERBOSELY)
          return None, None, None

      unit_prefix = unit_prefix.lower()

      # словарь с коэффициентами перевода единиц измерения.
      conversions: dict[str, float] = {
          "mg": 1,
          "gm": 1000,
          "g": 1000,
          "ng": 0.000001,
          "ug": 0.001,

          "ml": 1000,
          "nl": 0.001,                   # 1000 * 0.000001
          "ul": 1,                       # 1000 * 0.001

          "ppm": 24.45 / mw,             # 1 ppm = 1 mg/m3 * 24.45/mw
          "ppb": 0.001 * 24.45 / mw,     # 1 ppb = 0.001 ppm
          "pph": 1 / 60 * 24.45 / mw,    # 1 pph = 1/60 ppm
      }

      # переводим известные единицы к "mg/kg" и "mg/m3".
      if unit_prefix in conversions:
        num_dose *= conversions[unit_prefix]
        dose_unit = "mg/" + unit_suffix

      # если префикс не поддерживается.
      else:
        v_logger.warning(f"Unsupported dose_unit: {dose_unit}",
                         LogMode.VERBOSELY)
        return None, None, None

      return num_dose, dose_unit, time_per

    # применяем функцию извлечения дозы к каждой строке DataFrame.
    df[["numeric_dose", "dose_units", "time_period"]] = df.apply(
        lambda row: pd.Series(ExtractDose(row["dose"], row["mw"])),
        axis=1)
    # удаляем исходный столбец "dose" и переименовываем новый.
    df = df.drop(columns=["dose"]).rename(
        columns={"numeric_dose": "dose"})

    return df

  def SaveMolfileWithToxicityToSDF(df: pd.DataFrame, unit_type: str):
    """
    Сохраняет molfile соединения с данными о токсичности в SDF-файл.

    Args:
        df (pd.DataFrame): DataFrame, содержащий данные о токсичности соединения.
        unit_type (str): тип единиц измерения (например, "kg" или "m3").
    """

    # создаем пустой DataFrame для хранения данных.
    listed_df = pd.DataFrame()

    # итерируемся по столбцам DataFrame.
    for column_name in df.columns:
      # получаем данные столбца в виде списка.
      full_column_data = df[column_name].tolist()

      # добавляем данные столбца в новый DataFrame.
      listed_df[column_name] = [full_column_data]
      # если элемент уникален.
      if len(DedupedList(full_column_data)) == 1:
        # то записываем только его.
        listed_df.loc[0, column_name] = full_column_data[0]

    # сохраняем molfile в SDF-файл.
    SaveMolfilesToSDF(
        data=pd.DataFrame({"cid": [cid],
                           "molfile": [GetMolfileFromCID(cid)]}),
        file_name=(
            f"{toxicity_config["molfiles_folder_name"]}/"
            f"{compound_name}_{unit_type}"),
        molecule_id_column_name="cid",
        extra_data=listed_df,
        indexing_lists=True)

  def SaveToxicityUnitSpecification(compound_file_unit: str,
                                    unit_str: str,
                                    valid_units: list[str],
                                    acute_effects: pd.DataFrame):
    """
    Фильтрует, преобразует и сохраняет данные о токсичности для указанного
    типа единиц измерения.

    Args:
        compound_file_unit (str): имя файла для сохранения (без расширения).
        unit_str (str): тип единиц измерения ("kg" или "m3").
        valid_units (list[str]): список допустимых единиц измерения.
        acute_effects (pd.DataFrame): DataFrame с данными о токсичности.
    """

    v_logger.info("Filtering 'organism' and 'route'...",
                  LogMode.VERBOSELY)

    # фильтруем данные по организму.
    acute_effects_unit = acute_effects[acute_effects["organism"].isin(
        filtering_config[unit_str]["organism"])]
    # фильтруем данные по способу введения.
    acute_effects_unit = acute_effects_unit[
        acute_effects_unit["route"].isin(
            filtering_config[unit_str]["route"])]

    v_logger.success("Filtering 'organism' and 'route'!",
                     LogMode.VERBOSELY)

    v_logger.info(f"Filtering 'dose' in {unit_str}...",
                  LogMode.VERBOSELY)

    # если DataFrame пустой, пропускаем.
    if acute_effects_unit.empty:
      v_logger.warning(
          f"{compound_name}_{unit_str} is empty, no need saving, skip.",
          LogMode.VERBOSELY)
      return

    # если столбец "dose" присутствует.
    if "dose" in acute_effects_unit.columns:
      # извлекаем дозу, единицы измерения и время.
      acute_effects_unit = ExtractDoseAndTime(acute_effects_unit,
                                              valid_units)

      # преобразуем значения столбца "dose" в числовой формат.
      acute_effects_unit["dose"] = pd.to_numeric(
          acute_effects_unit["dose"], errors="coerce")

    # если столбец "dose" отсутствует.
    else:
      v_logger.warning(f"No dose in {compound_name}_{unit_str}, skip.",
                       LogMode.VERBOSELY)
      return

    # если DataFrame пустой, пропускаем.
    if acute_effects_unit.empty:
      v_logger.warning(
          f"{compound_name}_{unit_str} is empty, no need saving, skip.",
          LogMode.VERBOSELY)
      return

    # если столбцы "dose" или "dose_units" отсутствуют.
    if "dose" not in acute_effects_unit.columns or\
            "dose_units" not in acute_effects_unit.columns:
      v_logger.warning(
          f"{compound_name}_{unit_str} misses 'dose' or 'dose_units'"
          f", skip.",
          LogMode.VERBOSELY)
      return

    v_logger.success(f"Filtering 'dose' in {unit_str}!",
                     LogMode.VERBOSELY)

    v_logger.info(f"Adding 'pLD50' to {compound_name}_{unit_str}...",
                  LogMode.VERBOSELY)

    # вычисляем pLD50.
    acute_effects_unit["pLD50"] = -np.log10(
        (acute_effects_unit["dose"] / acute_effects_unit["mw"]) / 1000000)

    v_logger.success(f"Adding 'pLD50' to {compound_name}_{unit_str}!",
                     LogMode.VERBOSELY)

    v_logger.info(f"Saving {compound_name}_{unit_str} to .csv...",
                  LogMode.VERBOSELY)

    # заменяем пустые строки на NaN.
    acute_effects_unit = acute_effects_unit.replace('', np.nan)
    # удаляем столбцы, состоящие только из NaN.
    acute_effects_unit = acute_effects_unit.dropna(axis=1, how='all')

    # проверяем наличие дозы и единиц ее измерения.
    if "dose" in acute_effects_unit.columns and\
            "dose_units" in acute_effects_unit.columns:
      # оставляем только строки, в которых есть информация о дозе и
      # единицах измерения.
      acute_effects_unit =\
          acute_effects_unit[(acute_effects_unit['dose_units'].notna()
                              ) & (
              acute_effects_unit['dose'].notna())]

    # если нет нужных столбцов.
    else:
      v_logger.warning(
          f"{compound_name}_{unit_str} misses 'dose' or 'dose_units'"
          f", skip.",
          LogMode.VERBOSELY)
      return

    # сохраняем DataFrame в CSV-файл.
    acute_effects_unit.to_csv(f"{compound_file_unit}.csv",
                              sep=";",
                              index=False,
                              mode="w")

    v_logger.success(f"Saving {compound_name}_{unit_str} to .csv!",
                     LogMode.VERBOSELY)

    # если необходимо скачивать соединения в SDF.
    if toxicity_config["download_compounds_sdf"]:
      v_logger.info(f"Saving {compound_name}_{unit_str} to .sdf...",
                    LogMode.VERBOSELY)

      # создаем директорию для SDF-файлов.
      os.makedirs(toxicity_config["molfiles_folder_name"],
                  exist_ok=True)

      # сохраняем molfile в SDF-файл.
      SaveMolfileWithToxicityToSDF(acute_effects_unit, unit_str)

      v_logger.success(f"Saving {compound_name}_{unit_str} to .sdf!",
                       LogMode.VERBOSELY)

  v_logger.info("Adding 'mw'...", LogMode.VERBOSELY)

  # добавляем столбец с молекулярным весом.
  acute_effects = CalcMolecularWeight(acute_effects, "cid")

  try:
    # преобразуем значения столбца "mw" в числовой формат.
    acute_effects["mw"] = pd.to_numeric(acute_effects["mw"],
                                        errors="coerce")

    v_logger.success("Adding 'mw'!", LogMode.VERBOSELY)

  # если столбец "mw" не найден.
  except KeyError:
    v_logger.warning(f"No 'mw' for {compound_name}, skip.")
    return

  v_logger.info("~", LogMode.VERBOSELY)

  # сохраняем данные о токсичности для единиц измерения "kg".
  SaveToxicityUnitSpecification(compound_file_unit=compound_file_kg,
                                unit_str="kg",
                                valid_units=["gm/kg",
                                             "g/kg",

                                             "mg/kg",
                                             "ug/kg",
                                             "ng/kg",

                                             "mL/kg",
                                             "uL/kg",
                                             "nL/kg"],
                                acute_effects=acute_effects)

  v_logger.info("·", LogMode.VERBOSELY)

  # сохраняем данные о токсичности для единиц измерения "m3".
  SaveToxicityUnitSpecification(compound_file_unit=compound_file_m3,
                                unit_str="m3",
                                valid_units=["gm/m3",
                                             "g/m3",

                                             "mg/m3",
                                             "ug/m3",
                                             "ng/m3",

                                             "mL/m3",
                                             "uL/m3",
                                             "nL/m3",

                                             "ppm",
                                             "ppb",
                                             "pph"],
                                acute_effects=acute_effects)

  v_logger.info("·", LogMode.VERBOSELY)
  v_logger.success(f"Downloading {compound_name}!", LogMode.VERBOSELY)
  v_logger.info("-", LogMode.VERBOSELY)


# MARK: FilterDownloadedToxicityByCharacteristics


def FilterDownloadedToxicityByCharacteristics(unit_type: str,
                                              charact_1: str,
                                              charact_2: str,
                                              charact_3: str) -> None:
  """
  Фильтрует данные о токсичности из CSV-файла по заданным характеристикам,
  загружает molfile для каждого соединения и сохраняет результаты в CSV и SDF файлы.

  Args:
      unit_type (str): тип единиц измерения (например, "kg" или "m3").
      charact_1 (str): название первой характеристики для фильтрации.
      charact_2 (str): название второй характеристики для фильтрации.
      charact_3 (str): название третьей характеристики для фильтрации.
  """
  toxicity_config: Config = config["PubChem_download_toxicity"]

  v_logger.info(f"Filtering by characteristics for {unit_type}...")

  # папка для результатов фильтрации.
  charact_folder_name: str =\
      f"{toxicity_config["results_folder_name"]}/"\
      f"{filtering_config["characteristics_folder_name"]}"
  os.makedirs(charact_folder_name, exist_ok=True)

  # читаем CSV-файл с данными о токсичности.
  unit_type_df = pd.read_csv(f"{toxicity_config["results_folder_name"]}/"
                             f"{toxicity_config["combined_file_name"]}_{unit_type}.csv",
                             sep=config["csv_separator"],
                             low_memory=False)

  unique_charact_1 = unit_type_df[charact_1].unique()
  v_logger.info(f"Unique {charact_1}s: {unique_charact_1}.", LogMode.VERBOSELY)

  # заменяем отсутствующие значения времени на "no_exact_time".
  if charact_2 == "time_period":
    unit_type_df[charact_2] =\
        unit_type_df[charact_2].replace(np.nan, "no_exact_time")

  unique_charact_2 = unit_type_df[charact_2].unique()
  v_logger.info(f"Unique {charact_2}s: {unique_charact_2}.", LogMode.VERBOSELY)

  # словарь для хранения отфильтрованных данных.
  unique_separated: dict[str, dict[str, pd.DataFrame]] = {}

  # группируем данные по первой и второй характеристикам.
  for u_charact_1, charact_1_df in unit_type_df.groupby(charact_1):
    unique_separated.setdefault(str(u_charact_1), {})

    for u_charact_2, charact_2_df in charact_1_df.groupby(charact_2):
      unique_separated[str(u_charact_1)][str(u_charact_2)] =\
          charact_2_df.copy()

  unique_charact_3 = unit_type_df[charact_3].unique()
  v_logger.info(f"Unique {charact_3}s: {unique_charact_3}.", LogMode.VERBOSELY)

  # словарь для хранения окончательно отфильтрованных данных.
  unique_filtered: dict[str, dict[str, dict[str, pd.DataFrame]]] = {}

  # итерируемся по уникальным значениям характеристик и фильтруем данные.
  for u_charact_1 in unique_charact_1:
    v_logger.info("-", LogMode.VERBOSELY)
    v_logger.info(f"Current {charact_1}: {u_charact_1}.", LogMode.VERBOSELY)

    unique_filtered.setdefault(u_charact_1, {})

    for u_charact_2 in unique_charact_2:
      v_logger.info(f"Current {charact_2}: {u_charact_2}.", LogMode.VERBOSELY)

      unique_filtered[u_charact_1].setdefault(u_charact_2, {})

      df: pd.DataFrame
      try:
        # DataFrame для текущих значений первой и второй характеристик.
        df = unique_separated[u_charact_1][u_charact_2]

      except KeyError:
        # если DataFrame не существует, переходим к следующей итерации.
        continue

      for u_charact_3 in unique_charact_3:
        # фильтруем DataFrame по третьей характеристике.
        charact_df = df[df[charact_3] == u_charact_3].copy()

        filtered_file_name = f"{charact_folder_name}/{unit_type}/"\
            f"{toxicity_config["results_file_name"]}_"\
            f"{u_charact_1}_{u_charact_2}_{u_charact_3}"

        # если файл уже существует и скачивание пропущено, пропускаем.
        if os.path.exists(f"{filtered_file_name}.csv") and\
                config["skip_downloaded"]:
          v_logger.info(f"{u_charact_1}_{u_charact_2}_{u_charact_3}"
                        " is already downloaded, skip.",
                        LogMode.VERBOSELY)
          v_logger.info("~", LogMode.VERBOSELY)

          continue

        # устраняем дубликаты по 'cid', используя медиану 'dose'.
        charact_df = MedianDedupedDF(charact_df, "cid", "dose")

        # перевычисляем pLD50.
        if not charact_df.empty:
          charact_df["pLD50"] = -np.log10(
              (charact_df["dose"] / charact_df["mw"]) / 1000000)

        # если количество записей в отфильтрованном DataFrame больше или равно
        # filtering_config["occurrence_characteristics_number"], сохраняем его.
        if len(charact_df) >=\
                filtering_config["occurrence_characteristics_number"]:
          unique_filtered[u_charact_1][u_charact_2][u_charact_3] = charact_df

          os.makedirs(f"{charact_folder_name}/{unit_type}", exist_ok=True)

          charact_df.to_csv(f"{filtered_file_name}.csv")

          # если необходимо скачивать соединения в SDF.
          if toxicity_config["download_compounds_sdf"]:
            v_logger.info(f"Saving {unit_type} characteristics to .sdf...",
                          LogMode.VERBOSELY)

            cids: list[str] = list(charact_df["cid"])
            SaveMolfilesToSDF(data=pd.DataFrame(
                {"cid": cids,
                 "molfile": GetMolfilesFromCIDs(cids)}),
                file_name=filtered_file_name,
                molecule_id_column_name="cid",
                extra_data=charact_df,
                indexing_lists=True)

            v_logger.success(f"Saving {unit_type} characteristics to .sdf!",
                             LogMode.VERBOSELY)

          v_logger.success(
              f"Current {charact_3}: {u_charact_3}, len: "
              f"{len(unique_filtered[u_charact_1][u_charact_2][u_charact_3])}!",
              LogMode.VERBOSELY)
          v_logger.info("~", LogMode.VERBOSELY)

  v_logger.success(f"Filtering by characteristics for {unit_type}!")
