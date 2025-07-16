from AEOCFO.Utility.Logger_Utils import get_logger
from AEOCFO.Config.Folders import get_folder_id
from AEOCFO.Extract.Drive_Pull import drive_pull
from AEOCFO.Pipeline.Drive_Process import drive_process
from AEOCFO.Config.Folders import get_dataset_ids
from AEOCFO.Load.BQ_Push import bigquery_push
import re

if __name__ == "__main__":
    t = 'FICCOMBINE'
    logger = get_logger(t)
    logger.info(f"--- START PIPELINE: '{t}' ---")
    r = True

    drive = True
    bigquery = True

    OASIS_ID, CONTINGENCY_ID, FR_ID, FICCOMBINE_ID = get_folder_id(process=t)
    folder_ids = {
        'input': [OASIS_ID, CONTINGENCY_ID, FR_ID], 
        'output': FICCOMBINE_ID
    }
    
    years = ['FY25']
    for y in years:
        if r: print(f"FICCOMBINE Proccessing year: {y}")
        logger.info(f"FICCOMBINE Proccessing year: {y}")
        if drive:
            drive_process(directory_ids=folder_ids, process_type=t, duplicate_handling="Ignore", year=y, reporting=r)
        
        if bigquery:
            FICCOMBINE_DATASET_ID = get_dataset_ids(process_type=t)
            dataframes, names = drive_pull(FICCOMBINE_ID, process_type="BIGQUERY", reporting=r)
            if dataframes == {} and names == []:
                logger.info(f"No files of query type {t} found in designated folder ID {FICCOMBINE_ID}")
                raise 
            df_list = dataframes.values()
            name_list = names.values()
            bigquery_push(FICCOMBINE_DATASET_ID, df_list, name_list, processing_type=t, duplicate_handling="replace", reporting=r)

    logger.info(f"--- END PIPELINE: '{t}' ---")
        