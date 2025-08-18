import os
from pathlib import Path
import pandas as pd

def load_local_files(folder_path):
    folder = Path(folder_path)
    if not folder.exists():
        raise FileNotFoundError(f"Input folder '{folder}' does not exist.")
    
    dataframes = {}
    for file in folder.glob("*.csv"):
        dataframes[file.stem] = pd.read_csv(file)
    return dataframes