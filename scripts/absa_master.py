from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.http import MediaIoBaseUpload

import os
import io

import re
import pandas as pd
from ASUCExplore import Cleaning as cl
from ASUCExplore.Core import ABSA_Processor

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

def load_dataframes(folder_id, query_type='ALL', reporting=False) -> tuple[dict[str : pd.DataFrame], list[str]]:
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

# Processing Functions
class ASUCProcessor:
    """Wrapper class for processors. Specify the file type (eg. ABSA) then the __call__ method executes the appropriate processing function, outputting the result.
    The get_type method also outputs the type of processing (eg. ABSA processing pipeline) the ASUCProcessor instance was instructed to execute. 
    Both the actual output of processing (list of processed pd.DataFrame objects) and the type of processing initiated (self.type) are returned to an upload function. 
    
    Processing functions must take in:
    - df_dict (dict[str:pd.DataFrame]): dictionary where keys are file ids and values are the raw files converted into pandas dataframes.
    - reporting (str): parameter that tells the processing function whether or not to print outputs on processing progress.
    - names (dict[str:str]): dictionary where keys are file ids and values are raw file names.
    
    Processing functions must return:
    - list of processed pd.DataFrame objects
    - list of names with those failing naming conventions highighted
        - highlighting means we append 'MISMATCH' to the beginning the name
    
    Higher level architecture:
    - drive_pull func --> outputs raw files as dataframes and list of raw file names
    - ASUCProcessor instance 
        - takes in list of raw files names and raw fils as dataframes

        --> outputs processed fils in a list, type of processing executed and refined list of names with naming convention mismatches flagged
    - drive_push func:
        - From ASUCProcessor instance: take in the outputs of the processed files, the type of processing executed and updated list of names
        
        --> adjust the names of the files accodingly to indicate they're cleaned (based on raw file name and type of processing initiated) then upload files back into ocfo.database drive.

    Dependencies:
    - Currently depends on having ABSA_Processor from ASUCExplore > Core > ABSA_Processor.py alr imported into the file
    """
    naming_convention = {
        "ABSA" : ("RF", "GF") # ABSA processing outputs changes the "RF" raw file classification to the 'GF' general file classification, we don't need to tell the upload func to name the file ABSA because the raw fill should alr be named ABSA
    }

    def __init__(self, type: str):
        self.type = type
        self.processors = {
            "ABSA": self.absa,
            "Ficomm": self.ficomm
        }

    def get_type(self) -> str:
        return self.type

    def absa(self, df_dict, names, reporting = False) -> list[pd.DataFrame]:
        # need to check if df_dict and names are the same length but handle for case when name is a single string
        assert isinstance(df_dict, dict), f"df_dict is not a dictionary but {type(df_dict)}"
        assert cl.is_type(list(df_dict.keys()), str), f"df_dict keys are not all strings"
        assert cl.is_type(list(df_dict.values()), pd.DataFrame), f"df_dict values are not all pandas dataframes"

        assert isinstance(names, dict), f"names is not a dictionary but {type(names)}"
        assert cl.is_type(list(names.keys()), str), f"names keys are not all strings"
        assert cl.is_type(list(names.values()), str), f"names values are not strings"

        if not df_dict:
            raise ValueError("df_dict is empty! No DataFrames to process.")
        if not names:
            raise ValueError("names is empty! No file names to process.")
        
        df_lst = list(df_dict.values())
        id_lst = list(df_dict.keys())
        name_lst = list(names.values())

        rv = []
        for i in range(len(df_lst)):
            try: 
                df = df_lst[i]
                id = id_lst[i]
                name = name_lst[i]
                if self.get_type().lower() not in name.lower():
                    print(f"File does not matching processing naming conventions!\nFile name: {name}\nID: {id}") # do we raise to stop program or just print?
                    name_lst[i] = 'MISMATCH-' + name_lst[i] # WARNING: mutating array as we loop thru it, be careful
                rv.append(ABSA_Processor(df))
                if reporting:
                    print(f"Successfully ran ABSA_Processor on File: {name}, id: {id}")
            except Exception as e:
                raise e
        return rv, name_lst
    
    def ficomm(self, df_dict, names, reporting = False) -> list[pd.DataFrame]:
        return [], names # still being constructed
        
    # A little inspo from CS189 HW6
    def __call__(self, df_dict: dict[str, pd.DataFrame], names: dict[str, str], reporting: bool = False) -> list[pd.DataFrame]:
        """Call the appropriate processing function based on type."""
        if self.type not in self.processors:
            raise ValueError(f"Unsupported processing type '{self.type}'")
        return self.processors[self.type](df_dict, names, reporting) 

        
# Drive Push Functions
def upload_dataframe_to_drive(folder_id, df_list, names, processing_type, query_type, reporting=False) -> dict[str : str]:
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
    old_tag, new_tag = ASUCProcessor.naming_convention.get(processing_type, (None, "DEFAULT")) # use "DEFAULT" as default value if can't find processing type
    if old_tag is not None:
        pattern = rf"(.*)\-{old_tag}.{query_type}"
    else:
        pattern = rf"(.*).{query_type}"
    ids = {}
    for i in range(len(df_list)):
        df = df_list[i]
        if old_tag is not None:
            file_name = f"{re.match(pattern, names[i])}-{new_tag}" # clean out old tag and .query_type (eg. .csv)
        else:
            file_name = f"{re.match(pattern, names[i])}-{new_tag}" # just clean out .query_type (eg. .csv)
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

    dataframes, raw_names = load_dataframes(in_dir_id, query_type=qeury_type, reporting=reporting)
    
    processor = ASUCProcessor(process_type)       
    cleaned_dfs, cleaned_names = processor(dataframes, raw_names, reporting=reporting)
    processing_type = processor.get_type()

    df_ids: dict[str : str] = upload_dataframe_to_drive(out_dir_id, cleaned_dfs, cleaned_names, processing_type, query_type=qeury_type, reporting=reporting)

if __name__ == "__main__":
    ABSA_INPUT_FOLDER_ID = "1nlYOz8brWYgF3aKsgzpjZFIy1MmmEVxQ"
    ABSA_OUTPUT_FOLDER_ID = "1ELodPGvuV7UZRhTl1x4Phh0PzMDescyG"
    q = 'csv'
    report = True
    process(ABSA_INPUT_FOLDER_ID, ABSA_OUTPUT_FOLDER_ID, qeury_type=q, process_type='ABSA', reporting=True)
    

    