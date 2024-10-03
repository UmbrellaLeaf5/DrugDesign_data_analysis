from ChEMBL_download_compounds.download import DownloadChEMBLCompounds
# from PubChem_download.download import

if __name__ == "__main__":
    DownloadChEMBLCompounds(need_primary_analysis=True,
                            testing_flag=True, delete_downloaded_after_combining=False)
