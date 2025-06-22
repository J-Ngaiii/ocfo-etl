import pandas as pd
from tqdm import tqdm
from AEOCFO.Utility.Logger_Utils import get_logger
from AEOCFO.Utility.Authenticators import authenticate_drive
from AEOCFO.Utility.Drive_Helpers import list_files
from AEOCFO.Config.Drive_Config import get_process_config

PROCESS_CONFIG = get_process_config()

def drive_pull(folder_id: str, process_type: str, reporting=False) -> tuple[dict[str, pd.DataFrame | str], dict[str, str]]:
    """
    Pulls files for a given process type from a Google Drive folder and loads them.

    Returns:
    - dict[file_id] = processed file (DataFrame or str)
    - dict[file_id] = file name
    """
    logger = get_logger(process_type)
    logger.info(f"--- START: {process_type} drive_pull ---")

    assert process_type in PROCESS_CONFIG, f"Unsupported process_type '{process_type}'"

    config = PROCESS_CONFIG[process_type]
    query_type = config['query_type']
    handler = config['handler']

    files = list_files(folder_id, query_type=query_type, rv='FULL', reporting=reporting)
    if not files:
        logger.warning(f"No files found in designated extract folder {folder_id}")
        return {}, {}

    service = authenticate_drive()
    processed_data = {}
    id_to_name = {}

    for file in tqdm(files, desc="Pulling files from folder", ncols=100):
        file_id, file_name = file['id'], file['name']
        try:
            mime = service.files().get(fileId=file_id, fields="mimeType").execute().get("mimeType")
            proccessable_file = handler(file_id, mime, service)
            processed_data[file_id] = proccessable_file
            id_to_name[file_id] = file_name
            if reporting: print(f"Loaded: {file_name} ({file_id})")
            logger.info(f"Loaded: {file_name} ({file_id})")
        except Exception as e:
            logger.error(f"Error processing {file_name} ({file_id}): {str(e)}")

    logger.info(f"drive_pull successfully complete!")
    logger.info(f"--- END: {process_type} drive_pull ---")
    return processed_data, id_to_name


# Processing Functions: in ASUCExplore > Processor.py