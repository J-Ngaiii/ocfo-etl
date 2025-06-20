id_dict = {
    'ABSA_INPUT_FOLDER_ID' : "1nlYOz8brWYgF3aKsgzpjZFIy1MmmEVxQ", 
    'ABSA_OUTPUT_FOLDER_ID' : "1ELodPGvuV7UZRhTl1x4Phh0PzMDescyG", 
    'OASIS_INPUT_FOLDER_ID' : "1WwgSJoEyz1RKsdSJop3oSDlZ5LqZ_ZKz", 
    'OASIS_OUTPUT_FOLDER_ID' : "1r-1xZMnqYkp1BS59Mnq4Gs3z0yKeUAIL", 
    'CONTINGENCY_INPUT_FOLDER_ID' : "1pNoRbJgNeVacqr4RzTbYCi1DVZ1qiwzP", 
    'CONTINGENCY_OUTPUT_FOLDER_ID' : "1w1g7xvVNKSX2RgHVhuN9UsDB_HLUkLOj", 
    'OVERWRITE_FOLDER_ID' : '1j9cbFLhxlP4CeH1nfdVlHRNbkpXEwyzi'
}

def get_overwrite_folder_id():
    return id_dict['OVERWRITE_FOLDER_ID']

def get_absa_folder_id():
    return id_dict['ABSA_INPUT_FOLDER_ID'], id_dict['ABSA_OUTPUT_FOLDER_ID']

def get_oasis_folder_id():
    return id_dict['OASIS_INPUT_FOLDER_ID'], id_dict['OASIS_OUTPUT_FOLDER_ID']

def get_contingency_folder_id():
    return id_dict['CONTINGENCY_INPUT_FOLDER_ID'], id_dict['CONTINGENCY_OUTPUT_FOLDER_ID']

def get_all_ids():
    return id_dict