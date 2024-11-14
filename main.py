from ChEMBL_download_compounds.download import DownloadChEMBLCompounds
from ChEMBL_download_targets.download import DownloadChEMBLTargets
from ChEMBL_download_cell_lines.download import DownloadChEMBLCellLines

# from PubChem_download.download import

if __name__ == "__main__":
    DownloadChEMBLTargets(print_to_console_verbosely=True)
    DownloadChEMBLCellLines(print_to_console_verbosely=True)
