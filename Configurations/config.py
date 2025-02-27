import json
from typing import Any


Config = dict[str, Any]


config_file_name: str = "config.json"
main_config_file_name: str = f"Configurations/{config_file_name}"


def GetConfig(file_name: str = main_config_file_name,
              encoding: str = "utf-8"
              ) -> Config:
    with open(file_name, "r", encoding=encoding) as config:
        return json.load(config)


# MEANS: словарь, содержащий параметры конфигурации, используемые во многих функциях.
# (параметры конфигурации во многом определяют процесс скачивания)
config = GetConfig()
