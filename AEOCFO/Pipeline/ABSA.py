from AEOCFO.Pipeline.Drive_Process import process
from AEOCFO.Config.Folders import get_absa_folder_id

if __name__ == "__main__":
    ABSA_INPUT_FOLDER_ID, ABSA_OUTPUT_FOLDER_ID = get_absa_folder_id()
    q = 'csv'
    report = True
    process(ABSA_INPUT_FOLDER_ID, ABSA_OUTPUT_FOLDER_ID, process_type='ABSA', duplicate_handling="Ignore", reporting=True)