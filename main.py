"""
main.py:

Основной файл проекта, в котором вызываются все необходимые DrugDesign функции загрузки.
"""

# from ChEMBL_download_compounds.download import DownloadChEMBLCompounds
# ! Этот модуль импортирует DownloadChEMBLCompounds, но он не используется, поскольку
# ! данные о соединениях загружаются вместе с данными о мишенях и клеточных линиях.
# ! Отдельная загрузка соединений избыточна.

from ChEMBL_download_targets.download import DownloadChEMBLTargets
from ChEMBL_download_cell_lines.download import DownloadChEMBLCellLines

from PubChem_download_toxicity.download import DownloadPubChemCompoundsToxicity


if __name__ == "__main__":
    try:
        DownloadChEMBLCellLines()
        # DownloadChEMBLCompounds()
        DownloadChEMBLTargets()

        DownloadPubChemCompoundsToxicity()

    except KeyboardInterrupt:
        from Configurations.config import config
        print(config["keyboard_end_message"])
