from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.http import MediaIoBaseUpload

import os
import io

import re
import pandas as pd
from AEOCFO import Cleaning as cl
from AEOCFO import ASUCProcessor 

SCOPES = ["https://www.googleapis.com/auth/drive"]
API_NAME = "drive"
API_VERSION = "v3"
SERVICE_ACCOUNT_FILE = "credentials.json"  # Store credentials securely!

# Setup 
def _authenticate_drive():
    creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    return build(API_NAME, API_VERSION, credentials=creds)

# Drive Pull Functions
def _list_files(folder_id, query_type='ALL', rv='ID', reporting=False):
    """
    Given a google drive folder id, this function will return a list of all files from that folder that satisfy the 'qeury_type'.
    
    folder_id (str): ID of the folder from which to pull files from.
    query_type (str): Specifies what kind of files to pull. Default is 'ALL'.
        Currently only supports: 'ALL' and 'csv'
    rv (str): Specifies what attributes about each file to return. Default is 'ID' to return file ids. 
        Currently only supports 'ID', 'NAME' and 'PATH'
    reporting (bool): Specifies whether or not to turn on print statements to assist in debugging.
    """
    service = _authenticate_drive()
    match query_type:
        case 'ALL':
            print(f"Pulling all files from folder '{folder_id}'")
            query = f"'{folder_id}' in parents"
        case 'csv':
            print(f"Pulling all CSVs from folder '{folder_id}'")
            query = f"'{folder_id}' in parents and mimeType='text/csv'"
        case _:
            raise ValueError(f"Unsupported query type '{query_type}'. Please use either 'ALL' or 'csv'.")
    
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

def drive_pull(folder_id, query_type='ALL', reporting=False) -> tuple[dict[str : pd.DataFrame], list[str]]:
    """
    Pulls csv files from a Google Drive folder and loads them as Pandas DataFrames.

    folder_id (str): Google Drive folder ID.
    query_type (str): Specify file filter ('ALL' or 'csv').
    Returns:
    - dict of {file_id: DataFrame}
    - dict of {file_id: file name}
    """
    assert isinstance(folder_id, str), f"folder id must be a string but is a {type(folder_id)}"
    ids = _list_files(folder_id, query_type=query_type, rv='ID', reporting=reporting) # just make it return both
    names = _list_files(folder_id, query_type=query_type, rv='NAME', reporting=reporting)

    dataframes = {}
    id_names = {}
    for file_id, file_n in zip(ids, names):
        try:
            df = _download_drive_file(file_id, reporting=reporting)
            dataframes[file_id] = df
            id_names[file_id] = file_n
            print(f"Successfully loaded file into processing dictionary: {file_id}")
        except Exception as e:
            print(f"Error loading file {file_id} into processing dictionary: {e}")

    return dataframes, id_names

# Processing Functions: in ASUCExplore > Processor.py

        
# Drive Push Functions
def drive_push(folder_id, df_list, names, processing_type, query_type, reporting=False) -> dict[str : str]:
    """
    Uploads a Pandas DataFrame to Google Drive without saving it locally.

    Parameters:
    - folder_id (str): ID of the target Drive folder to upload files to.
    - df_list (list): The list of processed DataFrames.
    - names (str): Name of the file to be created in Google Drive.
    - processing_type (str): Type of processing done on files. (eg. ABSA Processing pipeline)
    - query_type (str): Type of files being processed (eg. csv files)



    Returns:
    - file_id (dict): The Names and ID of the uploaded file.
    """
    assert cl.is_type(df_list, pd.DataFrame), f"df_list is not a dataframe or list of dataframes"
    assert cl.is_type(names, str), f"names is not a string or list of strings"
    assert isinstance(processing_type, str), f"Processing type must be a single string specifying one type of processing done on all files fed into the function."
    
    if isinstance(df_list, pd.DataFrame):
        df_list = [df_list]
    if isinstance(names, str):
        names = [names] 

    
    service = _authenticate_drive()
    
    # Setup Regex pattern to clean out old identification tag for raw files (usually its just 'RF') and the file type (eg. cleaning out .csv at the end of the file name)
    old_tag, new_tag = ASUCProcessor.naming_convention.get(processing_type, (None, "DEFAULT")) # use "DEFAULT" as default value if can't find processing type
    if old_tag is not None:
        pattern = rf"(.*)\-{old_tag}.{query_type}" # clean out old tag and .query_type (eg. .csv)
    else:
        pattern = rf"(.*).{query_type}" # just clean out .query_type (eg. .csv)

    ids = {}
    for i in range(len(df_list)):
        df = df_list[i]
        file_name = f"{re.match(pattern, names[i]).group(1)}-{new_tag}" 

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

# Extract-Transform-Load Wrapper Func
def process(in_dir_id, out_dir_id, qeury_type, process_type = 'ABSA', reporting = False):
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

    dataframes, raw_names = drive_pull(in_dir_id, query_type=qeury_type, reporting=reporting)
    
    processor = ASUCProcessor(process_type)       
    cleaned_dfs, cleaned_names = processor(dataframes, raw_names, reporting=reporting)
    processing_type = processor.get_type()

    df_ids: dict[str : str] = drive_push(out_dir_id, cleaned_dfs, cleaned_names, processing_type, query_type=qeury_type, reporting=reporting)