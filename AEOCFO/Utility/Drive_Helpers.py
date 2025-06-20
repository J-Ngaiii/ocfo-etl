import pandas as pd
from googleapiclient.http import MediaIoBaseDownload
import io
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

def list_files(folder_id, query_type='ALL', rv='ID', reporting=False) -> list[str]:
    """
    Given a google drive folder id, this function will return a list of all files from that folder that satisfy the 'qeury_type'.
    
    folder_id (str): ID of the folder from which to pull files from.
    query_type (str): Specifies what kind of files to pull. Default is 'ALL'.
        Currently only supports: 'ALL', 'csv', 'gdoc' and 'txt'
    rv (str): Specifies what attributes about each file to return. Default is 'ID' to return file ids. 
        Currently only supports 'ID', 'NAME', 'PATH' and 'FUll'
        'FUll' returns a list of dictionaries where each dictionaries represents a file with the keys corresponding to id, name and path
    reporting (bool): Specifies whether or not to turn on print statements to assist in debugging.
    """
    service = authenticate_drive()
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
    raw_files = results.get("files", [])

    if rv == 'PATH':
        files = [f"https://drive.google.com/uc?id={file['id']}" for file in raw_files]
    elif rv == 'ID':
        files = [file['id'] for file in raw_files]
    elif rv == 'NAME':
        files = [file['name'] for file in raw_files]
    elif rv == 'FULL':
        files = [{'id': file['id'], 'name': file['name'], 'path': f"https://drive.google.com/uc?id={file['id']}"} for file in raw_files]
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


def download_text(file_id, mime_type, service) -> str:
    if mime_type == 'application/vnd.google-apps.document':
        request = service.files().export_media(fileId=file_id, mimeType='text/plain')
    elif mime_type == 'text/plain':
        request = service.files().get_media(fileId=file_id)
    else:
        raise ValueError(f"Unsupported MIME type '{mime_type}' for text export.")
    
    buffer = download_file_buffer(request)
    return buffer.read().decode('utf-8')