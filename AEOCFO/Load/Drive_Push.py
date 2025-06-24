from googleapiclient.http import MediaIoBaseUpload

import os
import io
from collections.abc import Iterable
from tqdm import tqdm

import re
import pandas as pd

from AEOCFO.Utility.Logger_Utils import get_logger
from AEOCFO.Utility.Cleaning import is_type
from AEOCFO.Transform import ASUCProcessor 
from AEOCFO.Utility.Drive_Helpers import get_unique_name_in_folder, list_files
from AEOCFO.Utility.Authenticators import authenticate_drive
from AEOCFO.Config.Folders import get_overwrite_folder_id

OVERWRITE_FOLDER_ID = get_overwrite_folder_id()

# Drive Push Functions
def drive_push(folder_id, df_list, names, processing_type, duplicate_handling = "Ignore", blind_to = None, archive_folder_id = OVERWRITE_FOLDER_ID, reporting=False) -> dict[str : str]:
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
        Overwrite: Replace files of the same name


    Returns:
    - file_id (dict): The Names and ID of the uploaded file.
    """
    logger = get_logger(processing_type)
    logger.info(f"--- START: {processing_type} drive_push (mode: {duplicate_handling}) ---")

    assert is_type(df_list, pd.DataFrame), f"df_list is not a dataframe or list of dataframes"
    assert is_type(names, str), f"names is not a string or list of strings"
    assert isinstance(processing_type, str), f"Processing type must be a single string specifying one type of processing done on all files fed into the function."
    if blind_to is not None:
        if isinstance(blind_to, str):
            blind_set = set([blind_to])
        elif isinstance(blind_to, Iterable):
            assert all(isinstance(name, str) for name in blind_to), f"All file names specified for this function to be blind to must be strings {blind_to}"
            blind_set = set(blind_to)
    else:
        blind_set = set()
    
    if isinstance(df_list, pd.DataFrame):
        df_list = [df_list]
    if isinstance(names, str):
        names = [names]   

    service = authenticate_drive()
    match duplicate_handling:
        case "Number":
            existing_names = set(list_files(folder_id=folder_id, query_type="ALL", rv="NAME", reporting=False)) # need to pull to check because inputted 'names' list will sometimes be different from names in google drive
            name_counter = dict(zip(existing_names, [1]*len(existing_names))) # assume that naturally the drive has only unique file names
            
            # Setup Regex pattern to clean out old identification tag for raw files (usually its just 'RF') and the file type (eg. cleaning out .csv at the end of the file name)

            ids = {}
            for i, df in tqdm(enumerate(df_list), desc="Uploading files to drive", ncols=100):
                base_name = os.path.splitext(names[i])[0] # splits file name from it's file type eg 'ABSA-FY25-RF.csv' --> 'ABSA-FY25-RF' and '.csv'
                final_name = base_name

                # Checking against blinded names
                if final_name in blind_set:
                    if reporting: print(f"drive_push blinded to file name {file_name}")
                    logger.info(f"drive_push blinded to file name {file_name}")
                    continue

                # Incrementing name
                if final_name in existing_names:
                    count = name_counter.get(file_name, 1) # default value is 1
                    while f"{file_name} ({count})" in existing_names:
                        count += 1
                    final_name = f"{file_name} ({count})"
                    name_counter[file_name] = count + 1

                # Checking again against blinded names
                if final_name in blind_set:
                    if reporting: print(f"drive_push blinded to file name {file_name}")
                    logger.info(f"drive_push blinded to file name {file_name}")
                    continue

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
                success_msg = f"\nSuccessfully uploaded {final_name} to Drive. File ID: {file.get('id')}"
                if reporting: print(success_msg)
                logger.info(success_msg)

        case "Ignore":
            existing_names = set(list_files(folder_id=folder_id, query_type="ALL", rv="NAME", reporting=False)) # need to pull to check because inputted 'names' list will sometimes be different from names in google drive

            ids = {}
            ignored_counts = 0
            for i, df in tqdm(enumerate(df_list), desc="Uploading files to drive", ncols=100):
                base_name = os.path.splitext(names[i])[0] # splits file name from it's file type eg 'ABSA-FY25-RF.csv' --> 'ABSA-FY25-RF' and '.csv'
                file_name = base_name

                # Checking against blinded names
                if file_name in blind_set:
                    if reporting: print(f"drive_push blinded to file name {file_name}")
                    logger.info(f"drive_push blinded to file name {file_name}")
                    continue

                # Ignoring 
                if file_name in existing_names:
                    if reporting:
                        print(f"\nIgnoring file {base_name}")
                        logger.info(f"Ignoring file {base_name}")
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
                success_msg = f"\nSuccessfully uploaded {file_name} to Drive. File ID: {file.get('id')}"
                if reporting: print(success_msg)
                logger.info(success_msg)
            if reporting: print(f"Uploaded {len(df_list) - ignored_counts} files, ignored {ignored_counts} files")
            logger.info(f"Uploaded {len(df_list) - ignored_counts} files, ignored {ignored_counts} files")

        case "Overwrite":
            assert archive_folder_id is not None, "archive_folder_id must be provided when using 'Overwrite' mode"
            existing_files = list_files(folder_id=folder_id, query_type="ALL", rv="FULL", reporting=False)
            name_to_fileid = {f['name']: f['id'] for f in existing_files}
            ids = {}
            overwrite_counts = 0
            for i, df in tqdm(enumerate(df_list), desc="Uploading files to drive", ncols=100):
                base_name = os.path.splitext(names[i])[0]
                file_name = base_name

                # Checking against blinded names
                if file_name in blind_set:
                    if reporting: print(f"drive_push blinded to file name {file_name}")
                    logger.info(f"drive_push blinded to file name {file_name}")
                    continue

                # Check for existing file
                if file_name in name_to_fileid:
                    old_file_id = name_to_fileid[file_name]
                    overwrite_counts += 1

                    # Rename old file with OVERWRITE- prefix and adds counts if overwrote name is not unique
                    base_archive_name = f"OVERWRITE-{file_name}"
                    unique_archive_name = get_unique_name_in_folder(service, archive_folder_id, base_archive_name)
                    service.files().update(
                        fileId=old_file_id, 
                        body={"name": unique_archive_name}
                    ).execute()

                    # Move to archive folder
                    service.files().update(
                        fileId=old_file_id,
                        addParents=archive_folder_id,
                        removeParents=folder_id,
                        fields='id, parents'
                    ).execute()

                    if reporting:
                        print(f"\nOverwrote and archived existing file: {file_name}")
                        logger.info(f"Overwrote and archived existing file: {file_name}")

                # Upload new file
                file_buffer = io.BytesIO()
                df.to_csv(file_buffer, index=False)
                file_buffer.seek(0)

                file_metadata = {
                    "name": file_name,
                    "parents": [folder_id],
                    "mimeType": "text/csv"
                }

                media = MediaIoBaseUpload(file_buffer, mimetype="text/csv")
                file = service.files().create(
                    body=file_metadata,
                    media_body=media,
                    fields="id"
                ).execute()

                ids[file_name] = file.get("id")
                success_msg = f"Successfully uploaded {file_name} to Drive. File ID: {file.get('id')}"
                if reporting: print(success_msg)
                logger.info(success_msg)

            if reporting: print(f"\nUploaded {len(df_list)} files, overwrote {overwrite_counts}")
            logger.info(f"Uploaded {len(df_list)} files, overwrote {overwrite_counts}")
            
        case _:
            error_msg = f"Unknown duplicate handling logic '{duplicate_handling}'."
            logger.error(error_msg)
            raise ValueError(error_msg)
        
    logger.info("drive_push successfully complete!")
    logger.info(f"--- END: {processing_type} drive_push ---")
    return ids
        