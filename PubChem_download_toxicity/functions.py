from io import StringIO
import json
import numpy as np
import re
import requests
import urllib.parse

from Utils.dataframe_funcs import DedupedList
from Utils.decorators import ReTry, time
from Utils.files_funcs import os, pd, SaveMolfilesToSDF
from Utils.verbose_logger import v_logger, LogMode

from Configurations.config import config, Config


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
                             page_folder_name: str):
    """
    Скачивает данные о токсичности соединения по информации из JSON PubChem
    и сохраняет их в CSV-файл.

    Args:
        compound_data (dict): словарь с информацией о соединении из JSON PubChem.
        page_folder_name (str): путь к директории, в которой будет сохранен файл.
    """

    toxicity_config: Config = config["PubChem_download_toxicity"]
    filtering_config: Config = toxicity_config["filtering"]

    cid: str = ""

    try:
        cid = compound_data["LinkedRecords"]["CID"][0]

    except KeyError:
        v_logger.warning(
            f"No 'cid' for 'sid': {compound_data["LinkedRecords"]["SID"][0]}, skip.")
        v_logger.warning(f"{"-" * 77}", LogMode.VERBOSELY)

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

    compound_file_kg = f"{page_folder_name.format(unit_type="kg")}/{compound_name}"
    compound_file_m3 = f"{page_folder_name.format(unit_type="m3")}/{compound_name}"

    if os.path.exists(f"{compound_file_kg}.csv") or\
       os.path.exists(f"{compound_file_m3}.csv") and\
       config["skip_downloaded"]:
        v_logger.info(f"{compound_name} is already downloaded, skip.",
                      LogMode.VERBOSELY)
        v_logger.warning(f"{"-" * 77}", LogMode.VERBOSELY)

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

    def ExtractDoseAndTime(df: pd.DataFrame,
                           valid_units: list[str]) -> pd.DataFrame:
        """
        Преобразует DataFrame с данными о дозировках, извлекая числовое значение, 
        единицу измерения и период времени.

        Args:
            df (pd.DataFrame): таблица с колонкой "dose", содержащей информацию о дозировках.
            valid_units (list[str]): список допустимых единиц измерения дозы.

        Returns:
            DataFrame с тремя новыми колонками: "numeric_dose", "dose_value", "time_period".
        """

        def ExtractDose(dose_str: str, mw: float) -> tuple[float | None, str | None, str | None]:
            if " " not in dose_str:
                return None, None, None

            num_dose: float | str | None = None
            dose_unit: str | None = None
            time_per: str | None = None

            try:
                if len(dose_str.split(" ")) != 2:
                    return None, None, None

                dose_amount_str, dose_and_time = dose_str.split(" ")
                num_dose = float(dose_amount_str)

            except ValueError:
                v_logger.warning(
                    f"Unsupported num_dose: {num_dose}, original: {dose_str}",
                    LogMode.VERBOSELY)
                return None, None, None

            match dose_str.count("/"):
                case 1:      # нету time period или это pp*/time
                    if dose_and_time.startswith("p"):
                        dose_unit, time_per = dose_and_time.split("/")
                    else:
                        dose_unit = dose_and_time
                        time_per = None

                case 2:      # есть time period
                    dose_unit = "/".join(dose_and_time.split("/")[:-1])
                    time_per = dose_and_time.split("/")[-1]

                case _:
                    return None, None, None

            if dose_unit not in valid_units:
                v_logger.warning(f"Unsupported dose_unit: {dose_unit}", LogMode.VERBOSELY)
                return None, None, None

            unit_prefix: str = dose_unit
            unit_suffix: str = "m3"

            if dose_unit.count("/") > 0:
                unit_prefix, unit_suffix = dose_unit.split("/")

                if unit_suffix not in ("kg", "m3"):
                    v_logger.warning(f"Unsupported dose_unit: {dose_unit}", LogMode.VERBOSELY)
                    return None, None, None

            unit_prefix = unit_prefix.lower()

            conversions: dict[str, float] = {
                "mg": 1,
                "gm": 1000,
                "g":  1000,
                "ng": 0.000001,
                "ug": 0.001,

                "ml": 1000,
                "nl": 0.001,  # 1000 * 0.000001
                "ul": 1,      # 1000 * 0.001

                "ppm": 24.45/mw,          # 1 ppm = 1 mg/m3 * 24.45/mw
                "ppb": 0.001 * 24.45/mw,  # 1 ppb = 0.001 ppm
                "pph": 1/60 * 24.45/mw,   # 1 pph = 1/60 ppm
            }

            # перевод известных единиц к "mg/kg" и "mg/m3"
            if unit_prefix in conversions:
                num_dose *= conversions[unit_prefix]
                dose_unit = "mg/" + unit_suffix

            else:
                v_logger.warning(f"Unsupported dose_unit: {dose_unit}", LogMode.VERBOSELY)
                return None, None, None

            return num_dose, dose_unit, time_per

        df[["numeric_dose", "dose_units", "time_period"]] = df.apply(
            lambda row: pd.Series(ExtractDose(row["dose"], row["mw"])), axis=1)
        df = df.drop(columns=["dose"]).rename(columns={"numeric_dose": "dose"})

        return df

    def SaveMolfileWithToxicityToSDF(df: pd.DataFrame, unit_type: str):
        listed_df = pd.DataFrame()
        for column_name in df.columns:
            full_column_data = df[column_name].tolist()

            listed_df[column_name] = [full_column_data]
            # в том случае если уникальный элемент только 1
            if len(DedupedList(full_column_data)) == 1:
                listed_df.loc[0, column_name] = full_column_data[0]

        molfile: str = GetResponse(
            f"https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/CID/{cid}/record/SDF?record_type=2d",
            True,
            None).text

        molfile = molfile[molfile.find("\n"):].replace("$$$$", "").rstrip()

        SaveMolfilesToSDF(data=pd.DataFrame({"cid": [cid],
                                             "molfile": [molfile]}),
                          file_name=(f"{toxicity_config["molfiles_folder_name"]}/"
                                     f"{compound_name}_{unit_type}"),
                          molecule_id_column_name="cid",
                          extra_data=listed_df,
                          indexing_lists=True)

    v_logger.info("Adding 'mw'...", LogMode.VERBOSELY)

    acute_effects = CalcMolecularWeight(acute_effects, "cid")

    try:
        acute_effects["mw"] = pd.to_numeric(acute_effects["mw"], errors="coerce")

        v_logger.success("Adding 'mw'!", LogMode.VERBOSELY)

    except KeyError:
        v_logger.warning(f"No 'mw' for {compound_name}.")

    v_logger.info("Filtering 'organism' and 'route'...", LogMode.VERBOSELY)

    acute_effects_kg = acute_effects[acute_effects["organism"].isin(
        filtering_config["kg"]["organism"])]
    acute_effects_kg = acute_effects_kg[acute_effects_kg["route"].isin(
        filtering_config["kg"]["route"])]

    acute_effects_m3 = acute_effects[acute_effects["organism"].isin(
        filtering_config["m3"]["organism"])]
    acute_effects_m3 = acute_effects_m3[acute_effects_m3["route"].isin(
        filtering_config["m3"]["route"])]

    v_logger.success("Filtering 'organism' and 'route'!", LogMode.VERBOSELY)

    v_logger.info("Filtering 'dose'...", LogMode.VERBOSELY)

    if not acute_effects_kg.empty:
        if "dose" in acute_effects_kg.columns:
            acute_effects_kg = ExtractDoseAndTime(acute_effects_kg, ["gm/kg",
                                                                     "g/kg",

                                                                     "mg/kg",
                                                                     "ug/kg",
                                                                     "ng/kg",

                                                                     "mL/kg",
                                                                     "uL/kg",
                                                                     "nL/kg"])

            acute_effects_kg["dose"] = pd.to_numeric(acute_effects_kg["dose"], errors="coerce")

        else:
            v_logger.warning(f"No dose in {compound_name}_kg!", LogMode.VERBOSELY)
            acute_effects_kg = pd.DataFrame({})

    if not acute_effects_m3.empty:
        if "dose" in acute_effects_m3.columns:
            acute_effects_m3 = ExtractDoseAndTime(acute_effects_m3, ["gm/m3",
                                                                     "g/m3",

                                                                     "mg/m3",
                                                                     "ug/m3",
                                                                     "ng/m3",

                                                                     "mL/m3",
                                                                     "uL/m3",
                                                                     "nL/m3",

                                                                     "ppm",
                                                                     "ppb",
                                                                     "pph"])

            acute_effects_m3["dose"] = pd.to_numeric(acute_effects_m3["dose"], errors="coerce")

        else:
            v_logger.warning(f"No dose in {compound_name}_m3!", LogMode.VERBOSELY)
            acute_effects_m3 = pd.DataFrame({})

    v_logger.success("Filtering 'dose'!", LogMode.VERBOSELY)

    if "mw" in acute_effects_kg.columns and "dose" in acute_effects_kg.columns and\
            not acute_effects_kg.empty:
        v_logger.info("Adding 'pLD50' to kg...", LogMode.VERBOSELY)

        acute_effects_kg["pLD50"] = -np.log10(
            (acute_effects_kg["dose"] / acute_effects_kg["mw"]) / 1000000)

        v_logger.success("Adding 'pLD50' to kg!", LogMode.VERBOSELY)

    if "mw" in acute_effects_m3.columns and "dose" in acute_effects_m3.columns and\
            not acute_effects_m3.empty:
        v_logger.info("Adding 'pLD50' to m3...", LogMode.VERBOSELY)

        acute_effects_m3["pLD50"] = -np.log10(
            (acute_effects_m3["dose"] / acute_effects_m3["mw"]) / 1000000)

        v_logger.success("Adding 'pLD50' to m3!", LogMode.VERBOSELY)

    if acute_effects_kg.empty:
        v_logger.warning(f"{compound_name}_kg is empty, no need saving.",
                         LogMode.VERBOSELY)

    if acute_effects_m3.empty:
        v_logger.warning(f"{compound_name}_m3 is empty, no need saving.",
                         LogMode.VERBOSELY)

    v_logger.info(f"Saving {compound_name} to .csv...", LogMode.VERBOSELY)

    if not acute_effects_kg.empty and "pLD50" in acute_effects_kg.columns:
        acute_effects_kg = acute_effects_kg.replace('', np.nan)
        acute_effects_kg = acute_effects_kg.dropna(axis=1, how='all')

        acute_effects_kg.to_csv(f"{compound_file_kg}.csv",
                                sep=";",
                                index=False,
                                mode="w")

    if not acute_effects_m3.empty and "pLD50" in acute_effects_m3.columns:
        acute_effects_m3 = acute_effects_m3.replace('', np.nan)
        acute_effects_m3 = acute_effects_m3.dropna(axis=1, how='all')

        acute_effects_m3.to_csv(f"{compound_file_m3}.csv",
                                sep=";",
                                index=False,
                                mode="w")

    v_logger.success(f"Saving {compound_name} to .csv!", LogMode.VERBOSELY)

    if toxicity_config["download_compounds_sdf"] and not acute_effects_kg.empty and\
            "pLD50" in acute_effects_kg.columns:
        os.makedirs(toxicity_config["molfiles_folder_name"], exist_ok=True)

        SaveMolfileWithToxicityToSDF(acute_effects_kg, "kg")

    if toxicity_config["download_compounds_sdf"] and not acute_effects_m3.empty and\
            "pLD50" in acute_effects_m3.columns:
        os.makedirs(toxicity_config["molfiles_folder_name"], exist_ok=True)

        SaveMolfileWithToxicityToSDF(acute_effects_m3, "m3")

    v_logger.success(f"Downloading {compound_name}!", LogMode.VERBOSELY)
    v_logger.info(f"{"-" * 77}", LogMode.VERBOSELY)
