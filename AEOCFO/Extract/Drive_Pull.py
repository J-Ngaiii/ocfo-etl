import pandas as pd
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
    assert process_type in PROCESS_CONFIG, f"Unsupported process_type '{process_type}'"

    config = PROCESS_CONFIG[process_type]
    query_type = config['query_type']
    handler = config['handler']

    files = list_files(folder_id, query_type=query_type, rv='FULL', reporting=reporting)
    if not files:
        return {}, {}

    service = authenticate_drive()
    processed_data = {}
    id_to_name = {}

    for file in files:
        file_id, file_name = file['id'], file['name']
        try:
            mime = service.files().get(fileId=file_id, fields="mimeType").execute().get("mimeType")
            result = handler(file_id, mime, service)
            processed_data[file_id] = result
            id_to_name[file_id] = file_name
            if reporting:
                print(f"Loaded: {file_name} ({file_id})")
        except Exception as e:
            print(f"Error processing {file_name} ({file_id}): {e}")

    return processed_data, id_to_name


# Processing Functions: in ASUCExplore > Processor.py