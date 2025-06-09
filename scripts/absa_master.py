from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.http import MediaIoBaseUpload

import os
import io

import pandas as pd
from ASUCExplore import Cleaning as cl
from ASUCExplore.Special import ABSA_Processor

SCOPES = ["https://www.googleapis.com/auth/drive"]
API_NAME = "drive"
API_VERSION = "v3"
SERVICE_ACCOUNT_FILE = "credentials.json"  # Store credentials securely!

def _authenticate_drive():
    creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    return build(API_NAME, API_VERSION, credentials=creds)

def _list_files(folder_id, query_type='ALL', rv='ID', reporting=False):
    """
    Given a google drive folder id, this function will return a list of all files from that folder that satisfy the 'qeury_type'.
    
    folder_id (str): ID of the folder from which to pull files from.
    query_type (str): Specifies what kind of files to pull. Default is 'ALL'.
        Currently only supports: 'ALL' and 'CSV'
    rv (str): Specifies what attributes about each file to return. Default is 'ID' to return file ids. 
        Currently only supports 'ID', 'NAME' and 'PATH'
    reporting (bool): Specifies whether or not to turn on print statements to assist in debugging.
    """
    service = _authenticate_drive()
    match query_type:
        case 'ALL':
            print(f"Pulling all files from folder '{folder_id}'")
            query = f"'{folder_id}' in parents"
        case 'CSV':
            print(f"Pulling all CSVs from folder '{folder_id}'")
            query = f"'{folder_id}' in parents and mimeType='text/csv'"
        case _:
            raise ValueError(f"Unsupported query type '{query_type}'. Please use either 'ALL' or 'CSV'.")
    
    #DEBUG: put the query into BigQuery to check that its valid if this bugs
    # print(f"Query is: {query}")

    results = service.files().list(q=query, fields="files(id, name)").execute()

    if reporting:
        files = []
        counts = 0
        for file in results.get("files", []):
            print(f"Found file with name: {file.get('name')}, ID: {file.get('id')}")
            counts += 1
            if rv == 'PATH':
                files.append(f"https://drive.google.com/uc?id={file['id']}")
            elif rv == 'ID':
                files.append(file.get("id"))
            elif rv == 'NAME':
                files.append(file.get("name"))
            else:
                raise ValueError(f"Unsupported return value '{rv}'. Please use either 'ID', 'NAME' or 'PATH'.")
        print(f"Process Complete with total files found: {counts}")
    else: 
        if rv == 'PATH':
            files = [f"https://drive.google.com/uc?id={file['id']}" for file in results.get("files", [])]
        elif rv == 'ID':
            files =  [file.get('id') for file in results.get("files", [])]
        elif rv == 'NAME':
            files =  [file.get('name') for file in results.get("files", [])]
        else:
            raise ValueError(f"Unsupported return value '{rv}'. Please use either 'ID', 'NAME' or 'PATH'.")
    return files

def _download_drive_file(file_id, reporting = False) -> pd.DataFrame:
    assert isinstance(file_id, str), f"file id must be a string but is a {type(file_id)}"
    
    service = _authenticate_drive()  # Ensure authentication
    request = service.files().get_media(fileId=file_id)

    # Stream file directly into memory
    file_buffer = io.BytesIO()
    downloader = MediaIoBaseDownload(file_buffer, request)
    
    done = False
    while not done:
        _, done = downloader.next_chunk()

    file_buffer.seek(0)  # Reset buffer position
    try:
        rv = pd.read_csv(file_buffer)  # Convert to DataFrame
        if reporting:
            print(f"Successfully converted {file_id} into a pandas dataframe")
        return rv
    except Exception as e:
        raise e

def load_dataframes(folder_id, query_type='ALL', reporting=False) -> dict[str : pd.DataFrame]:
    """
    Pulls CSV files from a Google Drive folder and loads them as Pandas DataFrames.

    folder_id (str): Google Drive folder ID.
    query_type (str): Specify file filter ('ALL' or 'CSV').
    Returns:
    - dict of {file_name: DataFrame}
    """
    assert isinstance(folder_id, str), f"folder id must be a string but is a {type(folder_id)}"
    files = _list_files(folder_id, query_type=query_type, rv='ID', reporting=reporting)

    dataframes = {}
    for file_id in files:
        try:
            df = _download_drive_file(file_id, reporting=reporting)
            dataframes[file_id] = df
            print(f"Successfully loaded file into processing dictionary: {file_id}")
        except Exception as e:
            print(f"Error loading file {file_id} into processing dictionary: {e}")

    return dataframes

def ABSA_process(df_dict, reporting = False) -> list[pd.DataFrame]:
    assert isinstance(df_dict, dict), f"df_dict is not a dictionary but {type(df_dict)}"
    assert cl.is_type(list(df_dict.keys()), str), f"df_dict keys are not all strings"
    assert cl.is_type(list(df_dict.values()), pd.DataFrame), f"df_dict values are not all pandas dataframes"

    if not df_dict:
        raise ValueError("df_dict is empty! No DataFrames to process.")
    
    if reporting:
        rv = []
        for i in range(len(df_dict.values())):
            try: 
                df = list(df_dict.values())[i]
                id = list(df_dict.keys())[i]
                rv.append(ABSA_Processor(df))
                print(f"Successfully ran ABSA_Processor on {id}")
            except Exception as e:
                raise e
    else:
        rv = [ABSA_Processor(df) for df in df_dict.values()]
    return rv
        

def upload_dataframe_to_drive(df_list, names, folder_id, reporting=False) -> dict[str : str]:
    """
    Uploads a Pandas DataFrame to Google Drive without saving it locally.

    Parameters:
    - df_list (list): The list of processed DataFrames
    - names (str): Name of the file to be created in Google Drive.
    - folder_id (str): ID of the target Drive folder.

    Returns:
    - file_id (dict): The Names and ID of the uploaded file.
    """
    assert cl.is_type(df_list, pd.DataFrame), f"df_list is not a dataframe or list of dataframes"
    assert cl.is_type(names, str), f"names is not a string or list of strings"
    
    if isinstance(df_list, pd.DataFrame):
        df_list = [df_list]
    if isinstance(names, str):
        names = [names] 

    service = _authenticate_drive()
    ids = {}
    for i in range(len(df_list)):
        df = df_list[i]
        file_name = names[i]
        # Convert DataFrame to CSV and write to an in-memory buffer
        file_buffer = io.BytesIO()
        df.to_csv(file_buffer, index=False)  # Save CSV content into memory
        file_buffer.seek(0)  # Reset buffer position

        # Prepare metadata
        file_metadata = {
            "name": file_name,
            "parents": [folder_id],
            "mimeType": "text/csv"
        }

        media = MediaIoBaseUpload(file_buffer, mimetype="text/csv")

        # Upload file
        file = service.files().create(
            body=file_metadata,
            media_body=media,
            fields="id"
        ).execute()

        ids[file_name] = file.get("id")
        if reporting:
            print(f"Successfully uploaded {file_name} to Drive. File ID: {file.get('id')}")
    return ids

if __name__ == "__main__":
    ABSA_INPUT_FOLDER_ID = "1LghLCxTTw_e7YNCEsKcXKaa0XMXbshBd"
    ABSA_OUTPUT_FOLDER_ID = "1Aj_liceFDsTwkSQc4gGvQXvKxCw1zbWC"
    q = 'CSV'
    dataframes: dict[str : pd.DataFrame] = load_dataframes(ABSA_INPUT_FOLDER_ID, query_type=q, reporting=True)
    cleaned_dfs: list[pd.DataFrame] = ABSA_process(dataframes, reporting=True)
    df_ids: dict[str : str] = upload_dataframe_to_drive(cleaned_dfs, 'ABSA-FY25-GF', ABSA_OUTPUT_FOLDER_ID, reporting=True)

    