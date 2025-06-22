from google.cloud import bigquery
import pandas as pd
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
        autodetect=True
    )

    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()  # Wait for the job to complete

    print(f"Uploaded {len(df)} rows to {table_ref}.")

def bigquery_push(dataset_id: str,
                  df_list: list[pd.DataFrame],
                  names: list[str],
                  processing_type: str,
                  duplicate_handling: str = "Ignore",
                  archive_dataset_id: str = OVERWRITE_DATASET_ID,
                  reporting: bool = False,
                  project_id: str = "your-default-project-id"):
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

    for df, name in zip(df_list, names):
        if reporting:
            print(f"[{processing_type}] Uploading '{name}' to dataset '{dataset_id}'...")

        # Optional: archive logic (stub)
        # TODO: Add logic to move current table to archive_dataset_id before overwrite

        push_table(df, project_id, dataset_id, name, if_exists="replace")

        if reporting:
            print(f"[{processing_type}] Finished uploading '{name}'.\n")
