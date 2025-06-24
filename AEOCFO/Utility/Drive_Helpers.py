import pandas as pd
import io
from collections.abc import Iterable
from googleapiclient.http import MediaIoBaseDownload
from AEOCFO.Utility.Authenticators import authenticate_drive


def get_unique_name_in_folder(service, archive_folder_id, base_name) -> str:
    """
    Returns a unique name in the archive folder by checking for existing files and appending (1), (2), etc.
    """
    query = f"'{archive_folder_id}' in parents and trashed = false"
    response = service.files().list(q=query, fields="files(name)").execute()
    existing_names = set(file['name'] for file in response.get('files', []))

    if base_name not in existing_names:
        return base_name

    # Find next available numbered name
    counter = 1
    while f"{base_name} ({counter})" in existing_names:
        counter += 1
    return f"{base_name} ({counter})"

def list_files(folder_id, query_type='ALL', rv='ID', name_keywords: Iterable[str] = None, reporting=False) -> list[str]:
    """
    Given a google drive folder id, this function will return a list of all files from that folder that satisfy the 'qeury_type'.
    
    folder_id (str): ID of the folder from which to pull files from.
    query_type (str): Specifies what kind of files to pull. Default is 'ALL'.
        Currently only supports: 'ALL', 'csv', 'gdoc', 'gspreadsheet' and 'txt'
    rv (str): Specifies what attributes about each file to return. Default is 'ID' to return file ids. 
        Currently only supports 'ID', 'NAME', 'PATH' and 'FUll'
        'FUll' returns a list of dictionaries where each dictionaries represents a file with the keys corresponding to id, name and path
    reporting (bool): Specifies whether or not to turn on print statements to assist in debugging.
    """
    service = authenticate_drive()
    query_type = query_type.lower()  # Normalize input

    mime_map = {
        'csv': "text/csv",
        'txt': "text/plain",
        'gdoc': "application/vnd.google-apps.document",
        'gspreadsheet': "application/vnd.google-apps.spreadsheet"
    }

    if '+' in query_type:
        query_parts = []
        for qt in query_type.split('+'):
            qt = qt.strip()
            if qt not in mime_map:
                raise ValueError(f"Unsupported query type part '{qt}'. Supported types: {list(mime_map.keys())}")
            query_parts.append(f"mimeType='{mime_map[qt]}'")
        mime_filter = " or ".join(query_parts)
        query = f"'{folder_id}' in parents and ({mime_filter})"
        print(f"Pulling files from folder '{folder_id}' with MIME types: {query_type}")
    else:
        match query_type:
            case 'all':
                query = f"'{folder_id}' in parents"
                print(f"Pulling all files from folder '{folder_id}'")
            case qt if qt in mime_map:
                query = f"'{folder_id}' in parents and mimeType='{mime_map[qt]}'"
                print(f"Pulling all {qt.upper()} files from folder '{folder_id}'")
            case _:
                raise ValueError(f"Unsupported query type '{query_type}'. Use 'ALL', 'csv', 'gdoc', 'txt', 'gspreadsheet', or combos like 'csv+gspreadsheet'.")
    
    #DEBUG: put the query into BigQuery to check that its valid if this bugs
    # print(f"Query is: {query}")

    results = service.files().list(q=query, fields="files(id, name, mimeType)").execute()
    if len(results) == 0:
        return []
    raw_files = results.get("files", [])

    if name_keywords: # default behavior is to set this to none and just pull everything
        assert all(isinstance(word, str) for word in name_keywords), f"not all inputted keywords to search for are strings: {name_keywords}"
        lower_keywords = [kw.lower() for kw in name_keywords]
        updated_raw_files = []
        for f in raw_files:
            name = f['name'].lower()
            if any(kw in name for kw in lower_keywords): # check for name contains
                updated_raw_files.append(f)
        raw_files = updated_raw_files

    if rv == 'PATH':
        files = [f"https://drive.google.com/uc?id={file['id']}" for file in raw_files]
    elif rv == 'ID':
        files = [file['id'] for file in raw_files]
    elif rv == 'NAME':
        files = [file['name'] for file in raw_files]
    elif rv == 'MIMETYPE':
        files = [file.get('mimeType') for file in raw_files]
    elif rv == 'FULL':
            files = [{
            'id': file['id'],
            'name': file['name'],
            'mimeType': file.get('mimeType'),
            'path': f"https://drive.google.com/uc?id={file['id']}"
        } for file in raw_files]
    elif rv == 'FILE':
        files = raw_files
    else:
        raise ValueError(f"Unsupported return value '{rv}'.")

    if reporting:
        for f in raw_files:
            print(f"Found file: {f['name']} (ID: {f['id']})")
        print(f"Process complete. Total files found: {len(files)}")
    return files

# ----------------------------
# Download Handlers
# ----------------------------

def download_file_buffer(request):
    """Download helper."""
    file_buffer = io.BytesIO()
    downloader = MediaIoBaseDownload(file_buffer, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    file_buffer.seek(0)
    return file_buffer


def download_csv(file_id, service) -> pd.DataFrame:
    request = service.files().get_media(fileId=file_id)
    buffer = download_file_buffer(request)
    return pd.read_csv(buffer)

def download_any_spreadsheet(file_id, mime_type, service, output='both') -> str:
    if mime_type == 'application/vnd.google-apps.spreadsheet':
        request = service.files().export_media(fileId=file_id, mimeType='text/csv')
    elif mime_type == 'text/csv':
        request = service.files().get_media(fileId=file_id)
    else:
        raise ValueError(f"Unsupported MIME type '{mime_type}' for csv export.")
    
    buffer = download_file_buffer(request)
    match output.lower():
        case 'both':
            buffer.seek(0)
            df = pd.read_csv(buffer)
            buffer.seek(0)
            text = buffer.read().decode('utf-8')
            return df, text
        case 'text':
            return buffer.read().decode('utf-8')
        case 'dataframe':
            return pd.read_csv(buffer)
        case _:
            raise ValueError(f"output type not supported {output}")

def download_text(file_id, mime_type, service) -> str:
    if mime_type == 'application/vnd.google-apps.document':
        request = service.files().export_media(fileId=file_id, mimeType='text/plain')
    elif mime_type == 'text/plain':
        request = service.files().get_media(fileId=file_id)
    else:
        raise ValueError(f"Unsupported MIME type '{mime_type}' for text export.")
    
    buffer = download_file_buffer(request)
    return buffer.read().decode('utf-8')