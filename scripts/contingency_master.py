from Drive import process

if __name__ == "__main__":
    CONTINGENCY_INPUT_FOLDER_ID = "1pNoRbJgNeVacqr4RzTbYCi1DVZ1qiwzP"
    CONTINGENCY_OUTPUT_FOLDER_ID = "1w1g7xvVNKSX2RgHVhuN9UsDB_HLUkLOj"
    q = 'txt'
    report = True
    process(CONTINGENCY_INPUT_FOLDER_ID, CONTINGENCY_OUTPUT_FOLDER_ID, process_type='Contingency', reporting=True)
    

    