from AEOCFO.Utility.Logger_Utils import get_logger
from AEOCFO.Transform.Processor import ASUCProcessor
from AEOCFO.Extract.Drive_Pull import drive_pull
from AEOCFO.Load.Drive_Push import drive_push

def process(in_dir_id, out_dir_id, process_type, duplicate_handling = "Ignore", reporting = False):
    """
    Handles the entire extract, transform and load process given an input and output dir id. Assumes implementation of an _authenticate() func to initiate service account.

    TO DO
    - figure out how to modify functions to return names so we can automatically name files
    - duplicate naming scheme
    """
    # dataframes: dict[str : pd.DataFrame]
    # raw_names: list[str]
    # --> go into ASUC Processor, which outputs --> 
    # cleaned_dfs: list[pd.DataFrame]
    # cleaned_names: list[str]  
    # --> go into upload func
    logger = get_logger(process_type)
    logger.info(f"--- START PIPELINE: '{process_type}' ---")

    dataframes, raw_names = drive_pull(in_dir_id, process_type=process_type, reporting=reporting)
    if dataframes == {} and raw_names == []:
        logger.info(f"No files of query type {process_type} found in designated folder ID{in_dir_id}")
        return
    
    logger.info(f"--- START: {process_type} ASUCProcessor ---")
    processor = ASUCProcessor(process_type)       
    cleaned_dfs, cleaned_names = processor(dataframes, raw_names, reporting=reporting)
    processing_type = processor.get_type()
    logger.info(f"ASUCProcessor successfully complete!")
    logger.info(f"--- END: {process_type} ASUCProcessor ---")

    df_ids: dict[str : str] = drive_push(out_dir_id, cleaned_dfs, cleaned_names, processing_type, duplicate_handling=duplicate_handling, reporting=reporting)

    logger.info(f"--- END PIPELINE: '{process_type}' ---\n")