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
def GetResponse(request_url: str,
                stream: bool,
                sleep_time: float
                ) -> requests.Response:
    """
    Отправляет GET-запрос по указанному URL, повторяет попытку в случае ошибки.

    Args:
        request_url (str): URL для запроса.
        stream (bool): если True, ответ будет получен потоком.
        sleep_time (float): время ожидания перед повторной попыткой в секундах.

    Returns:
        requests.Response: объект ответа requests.
    """
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
        res.encoding = 'utf-8'  # (UTF-8, если кодировка не указана)

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


def DownloadCompoundToxicity(compound_data: dict,
                             page_dir: str,
                             sleep_time: float,
                             skip_downloaded_files: bool,
                             print_to_console_verbosely: bool,
                             limit: int):
    """
    Скачивает данные о токсичности соединения по информации из JSON PubChem и сохраняет их в CSV-файл.

    Args:
        compound_data (dict): словарь с информацией о соединении из JSON PubChem.
        page_dir (str): путь к директории, в которой будет сохранен файл.
        sleep_time (float): время ожидания между запросами в секундах.
        skip_downloaded_files (bool): если True, то уже скачанные файлы пропускаются.
        print_to_console_verbosely (bool): если True, то в консоль выводится подробная информация о процессе скачивания.
        limit (int): максимальное количество объектов в запросе на скачивание.
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
                collection=table_info["collection"],
                limit=limit
            ),
            sleep_time=sleep_time,
        )

        # вот тут производить анализ и преобразования
        # TODO: оставить все столбцы
        # TODO: Dose: только число [mg/kg]
        # TODO: добавить MW: молекулярный вес каждого элемента
        # TODO: добавить pLD50 = -log10((Dose(mg/kg)/MW)/1000000)

        acute_effects.to_csv(f"{compound_filename}.csv", index=False, mode='w')

        if print_to_console_verbosely:
            logger.success(f"Downloading compound_{sid} toxicity: SUCCESS".ljust(77))

    except Exception as exception:
        LogException(exception)
