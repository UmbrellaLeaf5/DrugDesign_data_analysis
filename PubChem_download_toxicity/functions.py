from io import StringIO
import json
import numpy as np
import requests
import urllib.parse

from Utils.decorators import ReTry, time, Callable
from Utils.files_funcs import os, pd
from Utils.verbose_logger import v_logger, LogMode

from Configurations.config import Config


@ReTry()
def GetResponse(request_url: str,
                stream: bool,
                sleep_time: float | None
                ) -> requests.Response:
    """
    Отправляет GET-запрос по указанному URL, повторяет попытку в случае ошибки.

    Args:
        request_url (str): URL для запроса.
        stream (bool): если True, ответ будет получен потоком.
        sleep_time (float | None): время ожидания перед повторной попыткой в секундах.

    Returns:
        requests.Response: объект ответа requests.
    """
    if sleep_time is not None:
        time.sleep(sleep_time)

    response = requests.get(request_url, stream=stream)
    response.raise_for_status()

    return response


def GetDataFrameFromUrl(request_url: str, sleep_time: float) -> pd.DataFrame:
    """
    Скачивает данные из CSV-файла по URL и преобразует их в pandas DataFrame.

    Args:
        request_url (str): URL CSV-файла.
        sleep_time (float): время ожидания перед повторной попыткой в секундах.

    Returns:
        pd.DataFrame: DataFrame, содержащий данные из CSV-файла.
    """

    res = GetResponse(request_url, True, sleep_time)

    # определяем кодировку из заголовков ответа
    if res.encoding is None:
        res.encoding = "utf-8"  # (UTF-8, если кодировка не указана)

    return pd.read_csv(StringIO(res.content.decode(res.encoding)))


def GetLinkFromSid(sid: int,
                   collection: str,
                   limit: int
                   ) -> str:
    """
    Формирует URL для скачивания данных из PubChem SDQ API по SID (Structure ID).

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

        if not query:
            return ""

        return f"query={urllib.parse.quote(json.dumps(query))}"

    query = {
        "download": "*",
        "collection": f"{collection}",
        #  "order": ["relevancescore,desc"],
        "limit": f"{limit}",
        "where": {
            "ands": [
                {"sid": f"{sid}"}
            ]
        }
    }

    start = "https://pubchem.ncbi.nlm.nih.gov/sdq/sdqagent.cgi"\
        "?infmt=json"\
        "&outfmt=csv"

    return start + "&" + QueryDictToStr(query)


@ReTry(attempts_amount=1)
def DownloadCompoundToxicity(compound_data: dict,
                             page_folder_name: str,
                             config: Config):
    """
    Скачивает данные о токсичности соединения по информации из JSON PubChem
    и сохраняет их в CSV-файл.

    Args:
        compound_data (dict): словарь с информацией о соединении из JSON PubChem.
        page_folder_name (str): путь к директории, в которой будет сохранен файл.
        config (Config): словарь, содержащий параметры конфигурации для процесса скачивания.
    """

    toxicity_config: Config = config["PubChem_download_toxicity"]
    filtering_config: Config = toxicity_config["filtering"]

    try:
        compound_data["LinkedRecords"]["CID"][0]

    except KeyError:
        v_logger.warning(
            f"No 'cid' for 'sid': {compound_data["LinkedRecords"]["SID"][0]}, skip.")
        v_logger.info(f"{"-" * 77}", LogMode.VERBOSELY)

        return
        # не сохраняем те соединения, у которых нет cid,
        # так как невозможно вычислить молекулярные вес

    primary_sid: int | None
    try:
        primary_sid = int(compound_data["LinkedRecords"]["SID"][0])

    except KeyError:
        primary_sid = None

    raw_table: str = compound_data["Data"][0]["Value"]["ExternalTableName"]
    table_info: dict = {}

    for row in raw_table.split("&"):
        key, value = row.split("=")
        table_info[key] = value

    if table_info["query_type"] != "sid":
        v_logger.LogException(ValueError("Unknown query type at page "
                                         f"{page_folder_name}"))

    sid = int(table_info["query"])

    if primary_sid != sid:
        v_logger.warning(f"Mismatch between 'primary_sid' ({primary_sid}) "
                         f"and 'sid' ({sid}).")

    compound_name: str = f"compound_{sid}_toxicity"
    compound_file_name = f"{page_folder_name}/{compound_name}"

    if os.path.exists(f"{compound_file_name}.csv") and config["skip_downloaded"]:
        v_logger.info(f"{compound_name} is already downloaded, skip.",
                      LogMode.VERBOSELY)

        return

    v_logger.info(f"Downloading {compound_name}...", LogMode.VERBOSELY)

    acute_effects = GetDataFrameFromUrl(
        GetLinkFromSid(sid=sid,
                       collection=table_info["collection"],
                       limit=toxicity_config["limit"]),
        toxicity_config["sleep_time"]
    )

    @ReTry()
    def GetMolecularWeightByCid(cid) -> str:
        """
        Получает молекулярный вес соединения из PubChem REST API, используя его CID.

        Args:
            cid (int или str): PubChem Compound Identifier (CID) соединения.

        Returns:
            str: молекулярный вес соединения в виде строки.
        """

        return GetResponse("https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/cid/"
                           f"{cid}/property/MolecularWeight/txt",
                           True, None).text.strip()

    def CalcMolecularWeight(df: pd.DataFrame,
                            id_column: str,
                            ) -> pd.DataFrame:
        """
        Вычисляет и добавляет столбец 'mw' (молекулярный вес) в для pd.DataFrame.

        Args:
            df (pd.DataFrame): исходный pd.DataFrame.
            id_column (str): название столбца, содержащего ID соединений.

        Returns:
            pd.DataFrame: модифицированный DataFrame с добавленным столбцом 'mw'.
        """

        unique_ids = df[id_column].dropna().unique()

        if len(unique_ids) == 1:
            mw = GetMolecularWeightByCid(unique_ids[0])

            if mw is not None:
                df["mw"] = mw

                v_logger.info(f"Found 'mw' by '{id_column}'.", LogMode.VERBOSELY)

            else:
                v_logger.warning("Could not retrieve molecular weight by "
                                 f"'{id_column}' for {unique_ids[0]}.")

        elif len(unique_ids) == 0:
            v_logger.warning(f"No '{id_column}' found for {unique_ids[0]}.")

        else:
            v_logger.warning(f"Non-unique 'mw' by {id_column} for {unique_ids[0]}.")

            df["mw"] = df[id_column].apply(GetMolecularWeightByCid)

            if df["mw"].isnull().any():
                v_logger.warning(f"Some 'mw' could not be retrieved by {id_column}.")

        return df

    v_logger.info("Adding 'mw'...", LogMode.VERBOSELY)

    acute_effects = CalcMolecularWeight(acute_effects, "cid")

    try:
        acute_effects["mw"] = pd.to_numeric(acute_effects["mw"], errors="coerce")

        v_logger.success("Adding 'mw'!", LogMode.VERBOSELY)

    except KeyError:
        v_logger.warning(f"No 'mw' for {compound_name}.")

    v_logger.info("Filtering 'organism' and 'route'...", LogMode.VERBOSELY)

    acute_effects = acute_effects[acute_effects["organism"].isin(
        filtering_config["organism"])]
    acute_effects = acute_effects[acute_effects["route"].isin(
        filtering_config["route"])]

    v_logger.success("Filtering 'organism' and 'route'!", LogMode.VERBOSELY)
    v_logger.info("Filtering 'dose'...", LogMode.VERBOSELY)

    acute_effects = acute_effects[acute_effects["dose"].astype(
        str).str.lower().str.endswith(filtering_config["dose"])]

    acute_effects["dose"] = acute_effects["dose"].astype(
        str).str.extract(r"(\d+(?:\.\d+)?)", expand=False)

    acute_effects["dose"] = pd.to_numeric(acute_effects["dose"], errors="coerce")

    v_logger.success("Filtering 'dose'!", LogMode.VERBOSELY)

    if "mw" in acute_effects.columns:
        v_logger.info("Adding 'pLD50'...", LogMode.VERBOSELY)

        acute_effects["pLD50"] = -np.log10(
            (acute_effects["dose"] / acute_effects["mw"]) / 1000000)

        v_logger.success("Adding 'pLD50'!", LogMode.VERBOSELY)

    if not acute_effects.empty:
        v_logger.info(f"Saving {compound_name} to .csv...", LogMode.VERBOSELY)

        acute_effects.to_csv(f"{compound_file_name}.csv", sep=";",
                             index=False, mode="w")

        v_logger.success(f"Saving {compound_name} to .csv!", LogMode.VERBOSELY)
        v_logger.success(f"Downloading {compound_name}!", LogMode.VERBOSELY)

    else:
        v_logger.info(f"{compound_name} is empty, no need saving, skip.",
                      LogMode.VERBOSELY)

    v_logger.info(f"{"-" * 77}", LogMode.VERBOSELY)
