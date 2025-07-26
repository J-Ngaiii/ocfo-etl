from AEOCFO.Utility.Logger_Utils import get_logger
from AEOCFO.Pipeline.Drive_Process import drive_process
from AEOCFO.Config.Folders import get_folder_id, get_dataset_ids
from AEOCFO.Extract.Drive_Pull import drive_pull
from AEOCFO.Load.BQ_Push import bigquery_push
from AEOCFO.Config.Drive_Config import get_process_config

def main(t, verbose=True, drive=True, bigquery=False, testing=False, haltpush=True):
    """
    t (str): Processing type (eg. Contingency, OASIS, FR, etc).
    verbose (bool): Specifies whether or not to print logs fully.
    drive (bool): specifies whether or not run processing of raw files to a clean file in google drive
    bigquery (bool): specifies whether or not to 
    haltpush (bool): tells the function not to push files (helpful for debugging just pulling and processing functionalities)
    """
    assert t in get_process_config(), f"Inputted type '{t}' not supported. Supported types include: {get_process_config().keys()}"
    
    logger = get_logger(t)
    logger.info(f"--- START PIPELINE: '{t}' ---")

    if drive:
        INPUT_folderID, OUTPUT_folderID = get_folder_id(process=t, request='both', testing=testing)   
        folder_ids = {
            'input': INPUT_folderID, 
            'output': OUTPUT_folderID
        }
        drive_process(directory_ids=folder_ids, process_type=t, duplicate_handling="Ignore", reporting=verbose, testing=testing, haltpush=haltpush)

    if bigquery:
        DESTINATION_datasetID = get_dataset_ids(process_type=t, testing=testing)
        dataframes, names = drive_pull(OUTPUT_folderID, process_type="BIGQUERY", reporting=verbose)
        if not dataframes and not names:
            logger.warning(f"No files of query type {t} found in folder ID {OUTPUT_folderID}")
            raise ValueError(f"No BigQuery-ready files found for {t}")
        df_list = dataframes.values()
        name_list = names.values()
        bigquery_push(DESTINATION_datasetID, df_list, name_list, processing_type=t, duplicate_handling="replace", reporting=verbose)

    logger.info(f"--- END PIPELINE: '{t}' ---\n")
    