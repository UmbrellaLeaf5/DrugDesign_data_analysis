# from ChEMBL_download_compounds.download import DownloadChEMBLCompounds
from ChEMBL_download_targets.download import DownloadChEMBLTargets
from ChEMBL_download_cell_lines.download import DownloadChEMBLCellLines

from PubChem_download_toxicity.download import DownloadPubChemCompoundsToxicity

import json


if __name__ == "__main__":
    config: dict = {}

    with open("configurations.json", "r", encoding="utf-8") as f:
        config = json.load(f)

    # DownloadChEMBLCompounds(config)
    DownloadChEMBLCellLines(config)
    DownloadChEMBLTargets(config)

    DownloadPubChemCompoundsToxicity(config)
