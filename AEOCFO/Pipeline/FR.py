from AEOCFO.Utility.Logger_Utils import get_logger
from AEOCFO.Pipeline.Drive_Process import drive_process
from AEOCFO.Config.Folders import get_folder_ids
from AEOCFO.Extract.Drive_Pull import drive_pull
from AEOCFO.Config.BQ_Datasets import get_dataset_ids
from AEOCFO.Load.BQ_Push import bigquery_push


if __name__ == "__main__":
    t = 'FR'
    logger = get_logger(t)
    logger.info(f"--- START PIPELINE: '{t}' ---")
    r = True

    drive = True
    bigquery = True
    
    if drive:
        FR_INPUT_FOLDER_ID, FR_OUTPUT_FOLDER_ID = get_folder_ids(process_type=t, request='both')     
        drive_process(FR_INPUT_FOLDER_ID, FR_OUTPUT_FOLDER_ID, process_type=t, duplicate_handling="Ignore", reporting=r)

    if bigquery:
        FR_OUTPUT_FOLDER_ID = get_folder_ids(process_type=t, request='output')    
        FR_OUTPUT_DATASET_ID = get_dataset_ids(process_type=t)
        dataframes, names = drive_pull(FR_OUTPUT_FOLDER_ID, process_type="BIGQUERY", reporting=r)
        if dataframes == {} and names == []:
            logger.info(f"No files of query type {t} found in designated folder ID{FR_OUTPUT_FOLDER_ID}")
            raise 
        df_list = dataframes.values()
        name_list = names.values()
        bigquery_push(FR_OUTPUT_DATASET_ID, df_list, name_list, processing_type=t, duplicate_handling="replace")

    logger.info(f"--- END PIPELINE: '{t}' ---\n")