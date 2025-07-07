import pandas as pd
from tqdm import tqdm
from collections.abc import Iterable
from AEOCFO.Utility.Logger_Utils import get_logger
from AEOCFO.Utility.Authenticators import authenticate_drive
from AEOCFO.Utility.Drive_Helpers import list_files
from AEOCFO.Config.Drive_Config import get_process_config
from AEOCFO.Config.Folders import get_test_file_names

PROCESS_CONFIG = get_process_config()

def drive_pull(folder_id: str, process_type: str, name_keywords: Iterable[str] = None, reporting=False, debug=False, testing=False) -> tuple[dict[str, pd.DataFrame | str | tuple], dict[str, str]]:
    """
    Pulls files for a given process type from a Google Drive folder and loads them.

    Returns:
    - dict[file_id] = processed file (DataFrame, str, or tuple[DataFrame, str])
    - dict[file_id] = file name
    """
    logger = get_logger(process_type)
    logger.info(f"--- START: {process_type} drive_pull (Test Mode: {testing})---")

    assert process_type in PROCESS_CONFIG, f"Unsupported process_type '{process_type}'"

    config = PROCESS_CONFIG[process_type]
    query_type = config['query_type']
    handler = config['handler']

    if testing:
        test_keywords = get_test_file_names(process_type)
        if name_keywords is None:
            name_keywords = test_keywords
        else:
            name_keywords = list(name_keywords) + test_keywords
        logger.info(f"--- Pulling {process_type} test files and specified files: {name_keywords} ---")

    files = list_files(folder_id, query_type=query_type, rv='FULL', name_keywords=name_keywords, reporting=reporting)
    if not files:
        logger.warning(f"No files found in designated extract folder {folder_id}")
        return {}, {}

    service = authenticate_drive()
    processed_data = {}
    id_to_name = {}

    for file in tqdm(files, desc="Pulling files from folder", ncols=100):
        file_id = file['id']
        file_name = file['name']
        mime = file.get('mimeType')

        try:
            result = handler(file_id, mime, service) # will be a tuple containing dataframe and txt doc in teh case of processing_type = 'FR'
            if debug:
                print(f"DEBUG: drive_pull file_name, id, mimeType:{file_name}, {file_id}, {mime}")
                print(f"DEBUG: drive_pull result:\n{result}")
                
            processed_data[file_id] = result
            id_to_name[file_id] = file_name

            msg = f"Loaded: {file_name} ({file_id})"
            if reporting: print(f"\n{msg}")
            logger.info(msg)

        except Exception as e:
            if reporting: print(f"Error processing {file_name} ({file_id}): {str(e)}")
            logger.error(f"Error processing {file_name} ({file_id}): {str(e)}")

    if reporting: print("drive_pull successfully complete!")
    logger.info("drive_pull successfully complete!")
    logger.info(f"--- END: {process_type} drive_pull ---")
    return processed_data, id_to_name


# Processing Functions: in ASUCExplore > Processor.py