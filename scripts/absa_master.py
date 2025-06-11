from Drive import process

if __name__ == "__main__":
    ABSA_INPUT_FOLDER_ID = "1nlYOz8brWYgF3aKsgzpjZFIy1MmmEVxQ"
    ABSA_OUTPUT_FOLDER_ID = "1ELodPGvuV7UZRhTl1x4Phh0PzMDescyG"
    q = 'csv'
    report = True
    process(ABSA_INPUT_FOLDER_ID, ABSA_OUTPUT_FOLDER_ID, qeury_type=q, process_type='ABSA', reporting=True)
    

    