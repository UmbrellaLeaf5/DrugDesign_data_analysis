# from icecream import ic

import requests
import time
from io import StringIO
import json
import urllib.parse

from Utils.decorators import Retry
from Utils.file_and_logger_funcs import *

# ic.disable()


@Retry()
def GetResponse(request_url: str, stream: bool = False,
                sleep_time: float = 0.3) -> requests.Response:
    """
    Отправляет GET-запрос по указанному URL, повторяет попытку в случае ошибки.

    Args:
        request_url (str): URL для запроса.
        stream (bool, optional): если True, ответ будет получен потоком. Defaults to False.
        sleep_time (float, optional): время ожидания перед повторной попыткой в секундах. Defaults to 0.3.

    Returns:
        requests.Response: объект ответа requests.
    """
    time.sleep(sleep_time)

    response = requests.get(request_url, stream=stream)
    response.raise_for_status()

    return response


def GetDataFrameFromUrl(request_url: str, stream=True,
                        sleep_time: float = 0.3) -> pd.DataFrame:
    """
    Скачивает данные из CSV-файла по URL и преобразует их в pandas DataFrame.

    Args:
        request_url (str): URL CSV-файла.
        stream (bool, optional): если True, ответ будет получен потоком. Defaults to True.
        sleep_time (float, optional): время ожидания перед повторной попыткой в секундах. Defaults to 0.3.

    Returns:
        pd.DataFrame: DataFrame, содержащий данные из CSV-файла.
    """

    res = GetResponse(request_url, stream=stream, sleep_time=sleep_time)

    # определяем кодировку из заголовков ответа
    if res.encoding is None:
        res.encoding = 'utf-8'  # (UTF-8, если кодировка не указана)

    return pd.read_csv(StringIO(res.content.decode(res.encoding)))


def GetLinkFromSid(sid: int, limit: int = 10000000, collection: str = "chemidplus") -> str:
    """
    Формирует URL для скачивания данных из PubChem SDQ API по SID (Structure ID).

    Args:
        sid (int): SID соединения.
        limit (int, optional): максимальное количество возвращаемых записей. Defaults to 10000000.
        collection (str, optional): коллекция для поиска. Defaults to "chemidplus".

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


def DownloadCompoundToxicity(compound_data, page_dir: str,
                             logger_label: str = "PubChem_toxcomp",
                             label_color: str = "fg #AEBA66",
                             sleep_time: float = 0.3,
                             skip_downloaded_files: bool = False,
                             print_to_console_verbosely: bool = False):
    """
    Скачивает данные о токсичности соединения по информации из JSON PubChem и сохраняет их в CSV-файл.

    Args:
        compound_data (dict): словарь с информацией о соединении из JSON PubChem.
        page_dir (str): путь к директории, в которой будет сохранен файл.
        logger_label (str, optional): метка для логирования. Defaults to "PubChem_toxcomp".
        label_color (str, optional): цвет метки для логирования. Defaults to "fg #AEBA66".
        sleep_time (float, optional): время ожидания между запросами в секундах. Defaults to 0.3.
        skip_downloaded_files (bool, optional): если True, то уже скачанные файлы пропускаются. Defaults to False.
        print_to_console_verbosely (bool, optional): если True, то в консоль выводится подробная информация о процессе скачивания. Defaults to False.
    """

    try:
        cid = compound_data["LinkedRecords"]["CID"][0]
        compound_filename = f"{page_dir}/compound_{cid}_toxicity"

        if os.path.exists(compound_filename) and skip_downloaded_files:
            logger.info(f"Skipping existing file {compound_filename}".ljust(77))
            return

        raw_table = compound_data["Data"][0]["Value"]["ExternalTableName"]
        table_info: dict = {}

        for row in raw_table.split("&"):
            key, value = row.split("=")
            table_info[key] = value

        if table_info["query_type"] != "sid":
            logger.warning(f"Unknown query type at page {page_dir}".ljust(77))

        sid = int(table_info["query"])

        if print_to_console_verbosely:
            logger.info(f"Downloading compound_{sid} toxicity...".ljust(77))

        acute_effects = GetDataFrameFromUrl(
            GetLinkFromSid(
                sid=sid,
                collection=table_info["collection"]
            ),
            sleep_time=sleep_time
        )

        # вот тут производить анализ и преобразования

        acute_effects.to_csv(f"{compound_filename}.csv", index=False, mode='w')

        if print_to_console_verbosely:
            logger.success(f"Downloading compound_{sid} toxicity: SUCCESS".ljust(77))

    except Exception as exception:
        PrintException(exception, logger_label, label_color)
