from AEOCFO.Utility.Logger_Utils import get_logger
from AEOCFO.Pipeline.Drive_Process import drive_process
from AEOCFO.Config.Folders import get_folder_ids
from AEOCFO.Extract.Drive_Pull import drive_pull
from AEOCFO.Config.BQ_Datasets import get_dataset_ids
from AEOCFO.Load.BQ_Push import bigquery_push


if __name__ == "__main__":
    t = 'OASIS'
    logger = get_logger(t)
    logger.info(f"--- START PIPELINE: '{t}' ---")
    r = True
    drive = True
    bigquery = True
    
    if drive:
        OASIS_INPUT_FOLDER_ID, OASIS_OUTPUT_FOLDER_ID = get_folder_ids(process_type=t, request='both')
        folder_ids = {
            'input': OASIS_INPUT_FOLDER_ID, 
            'output': OASIS_OUTPUT_FOLDER_ID
        }
        drive_process(directory_ids=folder_ids, process_type=t, duplicate_handling="Ignore", reporting=r)

    if bigquery:
        OASIS_OUTPUT_DATASET_ID = get_dataset_ids(process_type=t)
        dataframes, names = drive_pull(OASIS_OUTPUT_FOLDER_ID, process_type="BIGQUERY", reporting=r)
        if dataframes == {} and names == []:
            logger.info(f"No files of query type {t} found in designated folder ID{OASIS_OUTPUT_FOLDER_ID}")
            raise 
        df_list = dataframes.values()
        name_list = names.values()
        bigquery_push(OASIS_OUTPUT_DATASET_ID, df_list, name_list, processing_type=t, duplicate_handling="replace", reporting=r)

    logger.info(f"--- END PIPELINE: '{t}' ---\n")