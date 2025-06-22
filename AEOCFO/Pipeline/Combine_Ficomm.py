from AEOCFO.Utility.Logger_Utils import get_logger
from AEOCFO.Config.Folders import get_folder_ids
from AEOCFO.Extract.Drive_Pull import drive_pull
from AEOCFO.Load.Drive_Push import drive_push
from AEOCFO.Config.BQ_Datasets import get_dataset_ids
from AEOCFO.Load.BQ_Push import bigquery_push
from AEOCFO.Pipeline.Ficomm_Process import process_weekly_pipeline
import re

def ficomm_process(year, process_type = 'FICCOMBINE', duplicate_handling = "Ignore", reporting = False):
    assert re.match(r'FY\d{2}', year) is not None, f"year must be formatted 'FY(year number)', currently {year}"
    
    
    logger = get_logger(t)
    logger.info(f"--- START FICCOMBINE YEAR: {year} ---")

    drive = True
    bigquery = True
    
    if drive:
        OASIS_ID, CONTINGENCY_ID, FR_ID, FICCOMBINE_ID = get_folder_ids(t=process_type)
        oasis_dict, oasis_names_dict = drive_pull(OASIS_ID, process_type=process_type, name_keywords=[year], reporting=reporting)
        contingency_dict, contingency_names_dict = drive_pull(CONTINGENCY_ID, process_type=process_type,  name_keywords=[year], reporting=reporting)
        fr_dict, fr_names_dict = drive_pull(FR_ID, process_type=process_type,  name_keywords=[year], reporting=reporting)

        oasis = oasis_dict.values()[0]
        frs = list(fr_dict.values())
        contingencies = list(contingency_dict.values())
        processed_dfs_dict, names = process_weekly_pipeline(oasis_df=oasis, fr_dfs=frs, cont_dfs=contingencies, threshold=0.9)

        drive_push(FICCOMBINE_ID, processed_dfs_dict, names, processing_type=process_type, duplicate_handling=duplicate_handling)

    if bigquery:
        FICCOMBINE_DATASET_ID = get_dataset_ids()
        dataframes, names = drive_pull(FICCOMBINE_ID, process_type="BIGQUERY", reporting=r)
        if dataframes == {} and names == []:
            logger.info(f"No files of query type {t} found in designated folder ID{FICCOMBINE_ID}")
            raise 
        df_list = dataframes.values()
        name_list = names.values()
        bigquery_push(FICCOMBINE_DATASET_ID, df_list, name_list, processing_type=process_type, duplicate_handling="replace")

    logger.info(f"--- END FICCOMBINE YEAR: {year} ---")

if __name__ == "__main__":
    t = 'FICCOMBINE'
    logger = get_logger(t)
    logger.info(f"--- START PIPELINE: {t} ---")
    r = True
    
    years = ['FY25']
    for y in years:
        ficomm_process(y, process_type=t, duplicate_handling="Ignore", reporting=r)