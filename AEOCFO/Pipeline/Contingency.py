from AEOCFO.Pipeline.Drive_Process import process
from AEOCFO.Config.Folders import get_contingency_folder_id

if __name__ == "__main__":
    CONTINGENCY_INPUT_FOLDER_ID, CONTINGENCY_OUTPUT_FOLDER_ID = get_contingency_folder_id()
    q = 'txt'
    report = True
    process(CONTINGENCY_INPUT_FOLDER_ID, CONTINGENCY_OUTPUT_FOLDER_ID, process_type='Contingency', duplicate_handling="Ignore", reporting=True)