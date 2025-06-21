from AEOCFO.Pipeline.Drive_Process import process
from AEOCFO.Config.Folders import get_fr_folder_id


if __name__ == "__main__":
    FR_INPUT_FOLDER_ID, FR_OUTPUT_FOLDER_ID = get_fr_folder_id()
    q = 'csv'
    report = True
    process(FR_INPUT_FOLDER_ID, FR_OUTPUT_FOLDER_ID, process_type='FR', duplicate_handling="Ignore", reporting=True)