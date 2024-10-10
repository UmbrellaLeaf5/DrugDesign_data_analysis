from ChEMBL_download_compounds.download import DownloadChEMBLCompounds
from ChEMBL_download_targets.download import DownloadChEMBLTargets

# from PubChem_download.download import

if __name__ == "__main__":
    DownloadChEMBLCompounds(skip_downloaded_files=True)
    DownloadChEMBLTargets(need_primary_analysis=True)
