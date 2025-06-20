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
        Currently only supports: 'ALL', 'csv', 'gdoc' and 'txt'
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
        case 'txt':
            print(f"Pulling all TXT files from folder '{folder_id}'")
            query = f"'{folder_id}' in parents and mimeType='text/plain'"
        case 'gdoc': 
            print(f"Pulling all Google Docs from folder '{folder_id}'")
            query = f"'{folder_id}' in parents and mimeType='application/vnd.google-apps.document'"
        case _:
            raise ValueError(f"Unsupported query type '{query_type}'. Please use either 'ALL' or 'csv'.")
    
    #DEBUG: put the query into BigQuery to check that its valid if this bugs
    # print(f"Query is: {query}")

    results = service.files().list(q=query, fields="files(id, name)").execute()

    if len(results) == 0:
        return []

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

def _download_drive_file(file_id, process_type, reporting=False) -> pd.DataFrame:
    assert isinstance(file_id, str), f"file id must be a string but is a {type(file_id)}"
    
    service = _authenticate_drive()  # Ensure authentication
    
    # First, get the file metadata to check MIME type
    file_metadata = service.files().get(fileId=file_id, fields="mimeType").execute()
    mime_type = file_metadata.get("mimeType")

    match process_type:
        case 'ABSA':
            # For ABSA, assume CSV files downloadable via get_media
            request = service.files().get_media(fileId=file_id)
            file_buffer = io.BytesIO()
            downloader = MediaIoBaseDownload(file_buffer, request)

            done = False
            while not done:
                _, done = downloader.next_chunk()

            file_buffer.seek(0)
            rv = pd.read_csv(file_buffer)  # Convert to DataFrame
            success_msg = f"Successfully converted {file_id} into a pandas dataframe"

        case 'Contingency':
            if mime_type == 'application/vnd.google-apps.document':
                # Export Google Doc as plain text
                request = service.files().export_media(fileId=file_id, mimeType='text/plain')
                file_buffer = io.BytesIO()
                downloader = MediaIoBaseDownload(file_buffer, request)

                done = False
                while not done:
                    _, done = downloader.next_chunk()

                file_buffer.seek(0)
                rv = file_buffer.read().decode('utf-8')
                success_msg = f"Successfully exported Google Doc {file_id} to txt"

            elif mime_type == 'text/plain':
                # For plain text files (.txt) or other, get_media works
                request = service.files().get_media(fileId=file_id)
                file_buffer = io.BytesIO()
                downloader = MediaIoBaseDownload(file_buffer, request)

                done = False
                while not done:
                    _, done = downloader.next_chunk()

                file_buffer.seek(0)
                rv = file_buffer.read().decode('utf-8')
                success_msg = f"Successfully downloaded txt file {file_id}"

            else:
                raise ValueError(f"Unsupported MIME type '{mime_type}' for Contingency process type. Files should either be .txt or google doc files")
            
        case 'OASIS':
            request = service.files().get_media(fileId=file_id)
            file_buffer = io.BytesIO()
            downloader = MediaIoBaseDownload(file_buffer, request)

            done = False
            while not done:
                _, done = downloader.next_chunk()

            file_buffer.seek(0)
            rv = pd.read_csv(file_buffer)  # Convert to DataFrame
            success_msg = f"Successfully converted {file_id} into a pandas dataframe"
        case _:
            raise ValueError(f"Unsupported process_type '{process_type}'")

    if reporting:
        print(success_msg)
    return rv


def drive_pull(folder_id, process_type, reporting=False) -> tuple[dict[str : pd.DataFrame], list[str]]:
    """
    Pulls files based on type of file being processed from a Google Drive folder and loads them as Pandas DataFrames.

    folder_id (str): Google Drive folder ID.
    Returns:
    - dict of {file_id: DataFrame}
    - dict of {file_id: file name}
    """
    assert isinstance(folder_id, str), f"folder id must be a string but is a {type(folder_id)}"
    match process_type:
        case 'ABSA':
            q = 'csv'
        case 'OASIS':
            q = 'csv'
        case 'Contingency':
            q = 'gdoc'
        case _:
            raise ValueError(f"Unsupported query type '{process_type}'. Please use either 'ABSA' or 'Contigency'.")
    ids = _list_files(folder_id, query_type=q, rv='ID', reporting=reporting) # just make it return both
    names = _list_files(folder_id, query_type=q, rv='NAME', reporting=reporting)

    if ids == [] and names == []:
        return {}, []

    files_for_processing = {}
    id_names = {}
    for file_id, file_name in zip(ids, names):
        try:
            file = _download_drive_file(file_id, process_type=process_type, reporting=reporting)
            files_for_processing[file_id] = file
            id_names[file_id] = file_name
            print(f"Successfully loaded file {file_name}, {file_id} ID into processing dictionary")
        except Exception as e:
            print(f"Error loading file {file_name}, {file_id} ID into processing dictionary")

    return files_for_processing, id_names

# Processing Functions: in ASUCExplore > Processor.py

        
# Drive Push Functions
def drive_push(folder_id, df_list, names, processing_type, duplicate_handling = "Ignore", reporting=False) -> dict[str : str]:
    """
    Uploads a Pandas DataFrame to Google Drive without saving it locally. Currently only handles for pushing CSV files to drive

    Parameters:
    - folder_id (str): ID of the target Drive folder to upload files to.
    - df_list (list): The list of processed DataFrames.
    - names (str): Name of the file to be created in Google Drive.
    - processing_type (str): Type of processing done on files. (eg. ABSA Processing pipeline)
    - duplicate_handling (str): Dictates how to handle uploading a file shares the same name with another file already in the target folder
        Ignore: Ignore the file, don't push it and move onto the next
        Number: Number the file then push it 


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
    match duplicate_handling:
        case "Number":
            existing_names = set(_list_files(folder_id=folder_id, query_type="ALL", rv="NAME", reporting=False)) # need to pull to check because inputted 'names' list will sometimes be different from names in google drive
            name_counter = dict(zip(existing_names, [1]*len(existing_names))) # assume that naturally the drive has only unique file names
            
            # Setup Regex pattern to clean out old identification tag for raw files (usually its just 'RF') and the file type (eg. cleaning out .csv at the end of the file name)
            old_tag, new_tag = ASUCProcessor.get_tagging().get(processing_type, (None, "DEFAULT")) # use "DEFAULT" as default value if can't find processing type
            ids = {}
            for i in range(len(df_list)):
                df = df_list[i]
                base_name = os.path.splitext(names[i])[0] # splits file name from it's file type eg 'ABSA-FY25-RF.csv' --> 'ABSA-FY25-RF' and '.csv'
                if old_tag:
                    base_name = re.sub(rf"\-{old_tag}$", "", base_name, flags=re.IGNORECASE)
                file_name = f"{base_name}-{new_tag}"
                
                final_name = file_name
                if final_name in existing_names:
                    count = name_counter.get(file_name, 1) # default value is 1
                    while f"{file_name} ({count})" in existing_names:
                        count += 1
                    final_name = f"{file_name} ({count})"
                    name_counter[file_name] = count + 1

                existing_names.add(final_name)

                # Convert DataFrame to CSV and write to an in-memory buffer
                file_buffer = io.BytesIO()
                df.to_csv(file_buffer, index=False) # Save CSV content into memory
                file_buffer.seek(0)  # Reset buffer position

                # Prepare metadata
                file_metadata = {
                    "name": final_name,
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

                ids[final_name] = file.get("id")
                if reporting:
                    print(f"Successfully uploaded {final_name} to Drive. File ID: {file.get('id')}")
            return ids
        case "Ignore":
            existing_names = set(_list_files(folder_id=folder_id, query_type="ALL", rv="NAME", reporting=False)) # need to pull to check because inputted 'names' list will sometimes be different from names in google drive
            
            # Setup Regex pattern to clean out old identification tag for raw files (usually its just 'RF') and the file type (eg. cleaning out .csv at the end of the file name)
            old_tag, new_tag = ASUCProcessor.get_tagging().get(processing_type, (None, "DEFAULT")) # use "DEFAULT" as default value if can't find processing type
            ids = {}
            ignored_counts = 0
            for i in range(len(df_list)):
                df = df_list[i]
                base_name = os.path.splitext(names[i])[0] # splits file name from it's file type eg 'ABSA-FY25-RF.csv' --> 'ABSA-FY25-RF' and '.csv'
                if old_tag:
                    base_name = re.sub(rf"\-{old_tag}$", "", base_name, flags=re.IGNORECASE)
                file_name = f"{base_name}-{new_tag}"
                
                if file_name in existing_names:
                    if reporting:
                        print(f"Ignoring file {base_name}")
                    ignored_counts += 1
                    continue

                existing_names.add(file_name)

                # Convert DataFrame to CSV and write to an in-memory buffer
                file_buffer = io.BytesIO()
                df.to_csv(file_buffer, index=False) # Save CSV content into memory
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
            if reporting:
                print(f"Uploaded {len(df_list) - ignored_counts} files, ignored {ignored_counts} files")
            return ids
        case _:
            raise ValueError("Unkown duplicate handling logic, use 'Ignore' or 'Number'.")
            
    

# Extract-Transform-Load Wrapper Func
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

    dataframes, raw_names = drive_pull(in_dir_id, process_type=process_type, reporting=reporting)
    if dataframes == {} and raw_names == []:
        print(f"No files of query type {process_type} found in designated folder ID{in_dir_id}")
        return
    
    processor = ASUCProcessor(process_type)       
    cleaned_dfs, cleaned_names = processor(dataframes, raw_names, reporting=reporting)
    processing_type = processor.get_type()

    df_ids: dict[str : str] = drive_push(out_dir_id, cleaned_dfs, cleaned_names, processing_type, duplicate_handling=duplicate_handling, reporting=reporting)