from Drive import process

if __name__ == "__main__":
    OASIS_INPUT_FOLDER_ID = "1WwgSJoEyz1RKsdSJop3oSDlZ5LqZ_ZKz"
    OASIS_OUTPUT_FOLDER_ID = "1r-1xZMnqYkp1BS59Mnq4Gs3z0yKeUAIL"
    q = 'csv'
    report = True
    process(OASIS_INPUT_FOLDER_ID, OASIS_OUTPUT_FOLDER_ID, process_type='OASIS', duplicate_handling="Ignore", reporting=True)