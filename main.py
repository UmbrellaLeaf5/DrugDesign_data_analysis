from ChEMBL_download.download import DownloadChEMBL
from ChEMBL_combine.combine import CombineChEMBL
# from PubChem_download.download import

if __name__ == "__main__":
    DownloadChEMBL(need_analysis=True, testing_flag=True)

    CombineChEMBL()
