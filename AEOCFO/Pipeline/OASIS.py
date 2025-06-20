from AEOCFO.Pipeline.Drive_Process import process
from AEOCFO.Config.Folders import get_oasis_folder_id


if __name__ == "__main__":
    OASIS_INPUT_FOLDER_ID, OASIS_OUTPUT_FOLDER_ID = get_oasis_folder_id()
    q = 'csv'
    report = True
    process(OASIS_INPUT_FOLDER_ID, OASIS_OUTPUT_FOLDER_ID, process_type='OASIS', duplicate_handling="Ignore", reporting=True)