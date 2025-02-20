import json
from typing import Any

from Utils.decorators import ReTry


Config = dict[str, Any]
LoggerConfig = dict[str, str]


config_file_name: str = "config.json"
main_config_file_name: str = f"Configurations/{config_file_name}"


@ReTry(attempts_amount=1)
def GetConfig(file_name: str,
              encoding: str = "utf-8"
              ) -> Config | None:
    with open(file_name, "r", encoding=encoding) as config:
        return json.load(config)
