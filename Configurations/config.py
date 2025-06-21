"""
Configurations/config.py

Этот модуль отвечает за загрузку конфигурации из JSON-файла.
"""

import json
from typing import Any


# создаем тип для конфигурации (словарь).
Config = dict[str, Any]

# имя файла конфигурации.
config_file_name: str = "config.json"
# путь к файлу конфигурации.
main_config_file_name: str = f"Configurations/{config_file_name}"


def GetConfig(file_name: str = main_config_file_name, encoding: str = "utf-8") -> Config:
  """
  Загружает конфигурацию из JSON-файла.

  Args:
      file_name (str): имя файла конфигурации.
      encoding (str): кодировка файла.

  Returns:
      Config: словарь с параметрами конфигурации.
  """

  # открываем файл конфигурации.
  with open(file_name, encoding=encoding) as config:
    # загружаем конфигурацию из JSON.
    return json.load(config)


# MEANS: словарь, содержащий параметры конфигурации, используемые во многих
# функциях (параметры конфигурации во многом определяют процесс скачивания).
config = GetConfig()
