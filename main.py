"""
main.py:

Основной файл проекта, в котором вызываются все необходимые DrugDesign функции загрузки.
"""

# from ChEMBL_download_compounds.download import DownloadChEMBLCompounds
# ! все соединения скачиваются в связке с целями/клеточными линиями,
# ! так что их отдельное скачивание избыточно и этот функционал,
# ! то есть ChEMBL_download_compounds, не используется.

from ChEMBL_download_targets.download import DownloadChEMBLTargets
from ChEMBL_download_cell_lines.download import DownloadChEMBLCellLines

from PubChem_download_toxicity.download import DownloadPubChemCompoundsToxicity

from Configurations.config import GetConfig


# MEANS: словарь, содержащий параметры конфигурации, используемые во многих функциях.
# (параметры конфигурации во многом определяют процесс скачивания)
config = GetConfig()

if __name__ == "__main__" and config is not None:
    try:
        DownloadChEMBLCellLines(config)
        # DownloadChEMBLCompounds(config)
        DownloadChEMBLTargets(config)

        DownloadPubChemCompoundsToxicity(config)

    except KeyboardInterrupt:
        print(config["keyboard_end_message"])
