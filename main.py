"""
main.py:

Основной файл проекта, в котором вызываются все необходимые DrugDesign функции загрузки.
"""

from ChEMBL_download_cell_lines.download import DownloadChEMBLCellLines
from ChEMBL_download_compounds.download import DownloadChEMBLCompounds
from ChEMBL_download_targets.download import DownloadChEMBLTargets
from Configurations.config import config
from PubChem_download_toxicity.download import DownloadPubChemCompoundsToxicity


download_tasks = {
  "ChEMBL_download_cell_lines": DownloadChEMBLCellLines,
  "ChEMBL_download_compounds": DownloadChEMBLCompounds,
  "ChEMBL_download_targets": DownloadChEMBLTargets,
  "PubChem_download_toxicity": DownloadPubChemCompoundsToxicity,
}

if __name__ == "__main__":
  try:
    for task, DownloadFunction in download_tasks.items():
      if config[task]["download"]:
        DownloadFunction()

  except KeyboardInterrupt:
    print(config["keyboard_end_message"])
