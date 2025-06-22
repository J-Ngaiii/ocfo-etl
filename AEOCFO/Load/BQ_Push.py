from google.cloud import bigquery
from tqdm import tqdm
import pandas as pd
from AEOCFO.Utility.Logger_Utils import get_logger
from AEOCFO.Utility.BQ_Helpers import col_name_conversion, clean_name
from AEOCFO.Config.BQ_Datasets import get_overwrite_dataset_id

OVERWRITE_DATASET_ID = get_overwrite_dataset_id()

def push_table(df: pd.DataFrame, project_id: str, dataset_id: str, table_id: str, if_exists: str = "replace"):
    """
    Uploads a DataFrame to BigQuery.

    Args:
        df (pd.DataFrame): DataFrame to upload.
        project_id (str): Google Cloud project ID.
        dataset_id (str): BigQuery dataset ID.
        table_id (str): BigQuery table ID.
        if_exists (str): 'replace', 'append', or 'fail'.
    """
    client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    job_config = bigquery.LoadJobConfig(
        write_disposition={
            "replace": bigquery.WriteDisposition.WRITE_TRUNCATE,
            "append": bigquery.WriteDisposition.WRITE_APPEND,
            "fail": bigquery.WriteDisposition.WRITE_EMPTY
        }[if_exists],
        autodetect=True # table automatically created if it doesn't alr exist
    )

    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()  # Wait for the job to complete

    print(f"Uploaded {len(df)} rows to {table_ref} (mode: {if_exists}).")

def bigquery_push(dataset_id: str,
                  df_list: list[pd.DataFrame],
                  names: list[str],
                  processing_type: str,
                  duplicate_handling: str = "replace",
                  archive_dataset_id: str = OVERWRITE_DATASET_ID,
                  reporting: bool = False,
                  project_id: str = "ocfo-primary"):
    """
    Pushes a list of DataFrames to BigQuery tables.

    Args:
        dataset_id (str): Target BigQuery dataset ID.
        df_list (list[pd.DataFrame]): List of DataFrames to push.
        names (list[str]): Corresponding table names.
        processing_type (str): Descriptive tag for logs/reports.
        duplicate_handling (str): Currently ignored — placeholder.
        archive_dataset_id (str): Dataset to archive overwritten tables (stub).
        reporting (bool): If True, print extra info.
        project_id (str): Google Cloud project ID.
    """
    if len(df_list) != len(names):
        raise ValueError("The number of dataframes and names must match.")
    
    logger = get_logger(processing_type)
    logger.info(f"--- START: {processing_type} bigquery_push ---")

    for df, name in tqdm(zip(df_list, names), desc="Pushing to bigqeury", ncols=100):
        if reporting: print(f"[{processing_type}] Uploading '{name}' to dataset '{dataset_id}'...")
        logger.info(f"[{processing_type}] Uploading '{name}' to dataset '{dataset_id}'...")

        # Optional: archive logic (stub)
        # TODO: Add logic to move current table to archive_dataset_id before overwrite

        df = col_name_conversion(df)[0]
        name = clean_name(name)
        push_table(df, project_id, dataset_id, name, if_exists=duplicate_handling)

        if reporting: print(f"[{processing_type}] Finished uploading '{name}'.\n")
        logger.info(f"[{processing_type}] Finished uploading '{name}'")

    logger.info(f"--- END: {processing_type} bigquery_push ---")
