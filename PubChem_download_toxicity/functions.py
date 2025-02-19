# from icecream import ic

import requests
from io import StringIO
import urllib.parse
import numpy as np


from Utils.decorators import ReTry, time, Callable
from Utils.file_and_logger_funcs import *

# ic.disable()


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


def GetDataFrameFromUrl(request_url: str,
                        sleep_time: float
                        ) -> pd.DataFrame:
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
            str: строка запроса в формате "query={JSON-encoded query}". Пустая строка, если словарь пуст.
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
                             sleep_time: float,
                             skip_downloaded_files: bool,
                             print_to_console_verbosely: bool,
                             limit: int):
    """
    Скачивает данные о токсичности соединения по информации из JSON PubChem и сохраняет их в CSV-файл.

    Args:
        compound_data (dict): словарь с информацией о соединении из JSON PubChem.
        page_folder_name (str): путь к директории, в которой будет сохранен файл.
        sleep_time (float): время ожидания между запросами в секундах.
        skip_downloaded_files (bool): если True, то уже скачанные файлы пропускаются.
        print_to_console_verbosely (bool): если True, то в консоль выводится подробная информация о процессе скачивания.
        limit (int): максимальное количество объектов в запросе на скачивание.
    """

    cid: int | None

    try:
        cid = int(compound_data["LinkedRecords"]["CID"][0])

    except KeyError:
        if print_to_console_verbosely:
            logger.warning(
                f"No 'cid' for 'sid': {compound_data["LinkedRecords"]["SID"][0]}, skip.")
            logger.info(f"{"-" * 77}")

        cid = None

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
        LogException(ValueError(f"Unknown query type at page {page_folder_name}"))

    sid = int(table_info["query"])

    if primary_sid != sid and print_to_console_verbosely:
        logger.warning(f"Mismatch between 'primary_sid' ({primary_sid}) "
                       f"and 'sid' ({sid}).")

    compound_name: str = f"compound_{sid}_toxicity"
    compound_filename = f"{page_folder_name}/{compound_name}"

    if os.path.exists(f"{compound_filename}.csv") and skip_downloaded_files:
        logger.info(f"Skipping existing file: {compound_name}.")
        return

    if print_to_console_verbosely:
        logger.info(f"Downloading {compound_name}...")

    acute_effects = GetDataFrameFromUrl(
        GetLinkFromSid(
            sid=sid,
            collection=table_info["collection"],
            limit=limit
        ),
        sleep_time=sleep_time,
    )

    # @ReTry()
    # def GetMolecularWeightByCid(cid):
    #     if pd.isna(cid) or not cid:
    #         return None

    #     return pcp.Compound.from_cid(str(cid)).molecular_weight

    # @ReTry()
    # def GetMolecularWeightBySid(sid):
    #     if pd.isna(sid) or not sid:
    #         return None

    #     cids = pcp.Substance.from_sid(str(sid)).cids
    #     if len(cids):
    #         return GetMolecularWeightByCid(cids[0])

    #     else:
    #         return None

    @ReTry()
    def GetMolecularWeightByCidPugRest(cid):
        return GetResponse("https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/cid/"
                           f"{cid}/property/MolecularWeight/txt",
                           True, None).text.strip()

    def CalcMolecularWeight(df: pd.DataFrame,
                            id_column: str,
                            weight_function: Callable,
                            ) -> pd.DataFrame:
        unique_ids = df[id_column].dropna().unique()

        if len(unique_ids) == 1:
            mw = weight_function(unique_ids[0])

            if mw is not None:
                df["mw"] = mw

                if print_to_console_verbosely:
                    logger.info(f"Found 'mw' by '{id_column}'.")

            else:
                logger.warning("Could not retrieve molecular weight by "
                               f"'{id_column}' for {unique_ids[0]}")

                return df

        elif len(unique_ids) == 0:
            logger.warning(f"No '{id_column}' found for.")
            return df

        else:
            if print_to_console_verbosely:
                logger.warning(f"Non-unique 'mw' by {id_column} for.")

            df["mw"] = df[id_column].apply(weight_function)

            if df["mw"].isnull().any():
                logger.warning(f"Some 'mw' could not be retrieved by {id_column}.")

        return df

    if print_to_console_verbosely:
        logger.info("Adding 'mw'...")

    acute_effects = CalcMolecularWeight(
        acute_effects,
        "cid",
        GetMolecularWeightByCidPugRest,
    )

    try:
        acute_effects["mw"] = pd.to_numeric(acute_effects["mw"], errors="coerce")

        if print_to_console_verbosely:
            logger.success("Adding 'mw'!")

    except KeyError:
        logger.warning("No 'mw'.")

    if print_to_console_verbosely:
        logger.info("Filtering 'organism' and 'route'...")

    acute_effects = acute_effects[acute_effects["organism"].isin(["rat", "mouse"])]
    acute_effects = acute_effects[acute_effects["route"].isin(
        ["oral", "intraperitoneal", "intravenous", "subcutaneous"])]

    if print_to_console_verbosely:
        logger.success("Filtering 'organism' and 'route'!")

        logger.info("Filtering 'dose'...")

    acute_effects = acute_effects[acute_effects["dose"].astype(
        str).str.lower().str.endswith("mg/kg")]

    acute_effects["dose"] = acute_effects["dose"].astype(
        str).str.extract(r"(\d+(?:\.\d+)?)", expand=False)

    acute_effects["dose"] = pd.to_numeric(acute_effects["dose"], errors="coerce")

    if print_to_console_verbosely:
        logger.success("Filtering 'dose'!")

    if "mw" in acute_effects.columns:
        if print_to_console_verbosely:
            logger.info("Adding 'pLD50'...")

        acute_effects["pLD50"] = -np.log10(
            (acute_effects["dose"] / acute_effects["mw"]) / 1000000)

        if print_to_console_verbosely:
            logger.success("Adding 'pLD50'!")

    if not acute_effects.empty:
        if print_to_console_verbosely:
            logger.info(f"Saving {compound_name} to .csv...")

        acute_effects.to_csv(f"{compound_filename}.csv", sep=";",
                             index=False, mode="w")

        if print_to_console_verbosely:
            logger.success(f"Saving {compound_name} to .csv!")

            logger.success(f"Downloading {compound_name}!")

    else:
        if print_to_console_verbosely:
            logger.warning(f"{compound_name} is empty, no need saving.")

    if print_to_console_verbosely:
        logger.info(f"{"-" * 77}")
