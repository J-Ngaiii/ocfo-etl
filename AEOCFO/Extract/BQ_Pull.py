from google.cloud import bigquery
import pandas as pd

def pull_from_bigquery(project_id: str, query: str) -> pd.DataFrame:
    """
    Executes a SQL query on BigQuery and returns the result as a DataFrame.

    Args:
        project_id (str): Google Cloud project ID.
        query (str): SQL query string.

    Returns:
        pd.DataFrame: Query result as a DataFrame.
    """
    client = bigquery.Client(project=project_id)
    df = client.query(query).to_dataframe()
    return df