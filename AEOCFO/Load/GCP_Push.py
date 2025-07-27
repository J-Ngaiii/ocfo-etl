from google.cloud import storage
from AEOCFO.Utility.Logger_Utils import get_logger
from AEOCFO.Config.Authenticators import authenticate_credentials
from AEOCFO.Utility.BQ_Helpers import clean_name
from AEOCFO.Config.Folders import get_overwrite_bucket_id

import pandas as pd
from io import StringIO
from tqdm import tqdm
import os

OVERWRITE_BUCKET_ID = get_overwrite_bucket_id()

def push_df_to_gcs(df: pd.DataFrame, bucket_name: str, destination_blob_name: str, project_id: str):
    """
    Uploads a DataFrame to GCS as a CSV.

    Args:
        df (pd.DataFrame): DataFrame to upload.
        bucket_name (str): GCS bucket name.
        destination_blob_name (str): Path in bucket.
        project_id (str): GCP project ID.
    """
    creds = authenticate_credentials(acc='pusher', platform='googlecloud')
    client = storage.Client(project=project_id, credentials=creds)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    blob.upload_from_string(csv_buffer.getvalue(), content_type="text/csv")

    print(f"Uploaded DataFrame to gs://{bucket_name}/{destination_blob_name}")


def gcs_push_from_dfs(bucket_id: str,
                      df_list: list[pd.DataFrame],
                      names: list[str],
                      processing_type: str,
                      duplicate_handling: str = "replace",
                      archive_bucket_id: str = OVERWRITE_BUCKET_ID,
                      reporting: bool = False,
                      project_id: str = "ocfo-primary"):
    """
    Pushes a list of DataFrames to Google Cloud Storage as CSVs.

    Args:
        bucket_id (str): Target GCS bucket ID.
        df_list (list[pd.DataFrame]): DataFrames to upload.
        names (list[str]): Corresponding GCS blob names.
        processing_type (str): Tag for logging.
        duplicate_handling (str): Currently ignored.
        archive_bucket_id (str): Bucket to archive old blobs (stub).
        reporting (bool): If True, print status updates.
        project_id (str): GCP project ID.
    """
    if len(df_list) != len(names):
        raise ValueError("The number of DataFrames and names must match.")

    logger = get_logger(processing_type)
    logger.info(f"--- START: {processing_type} gcs_push_from_dfs (mode: {duplicate_handling}) ---")

    for df, name in tqdm(zip(df_list, names), desc="Pushing to GCS", ncols=100):
        if not isinstance(df, pd.DataFrame):
            logger.error(f"[{processing_type}] Invalid input: Expected DataFrame, got {type(df)}")
            raise TypeError(f"Expected DataFrame, got {type(df)}")

        name = clean_name(name)
        if reporting:
            print(f"[{processing_type}] Uploading '{name}' to bucket '{bucket_id}'...")
        logger.info(f"[{processing_type}] Uploading '{name}' to bucket '{bucket_id}'...")

        push_df_to_gcs(df, bucket_id, name, project_id)

        if reporting:
            print(f"[{processing_type}] Finished uploading '{name}'.\n")
        logger.info(f"[{processing_type}] Finished uploading '{name}'")

    if reporting:
        print(f"Successfully pushed {len(df_list)} file(s) to GCS {project_id}.{bucket_id}")
    logger.info(f"Successfully pushed {len(df_list)} file(s) to GCS {project_id}.{bucket_id}")
    logger.info(f"--- END: {processing_type} gcs_push_from_dfs (mode: {duplicate_handling}) ---")

