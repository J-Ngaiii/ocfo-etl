id_dict = {
    'ABSA_INPUT_FOLDER_ID' : "1nlYOz8brWYgF3aKsgzpjZFIy1MmmEVxQ", 
    'ABSA_OUTPUT_FOLDER_ID' : "1ELodPGvuV7UZRhTl1x4Phh0PzMDescyG", 
    'OASIS_INPUT_FOLDER_ID' : "1WwgSJoEyz1RKsdSJop3oSDlZ5LqZ_ZKz", 
    'OASIS_OUTPUT_FOLDER_ID' : "1r-1xZMnqYkp1BS59Mnq4Gs3z0yKeUAIL", 
    'CONTINGENCY_INPUT_FOLDER_ID' : "1pNoRbJgNeVacqr4RzTbYCi1DVZ1qiwzP", 
    'CONTINGENCY_OUTPUT_FOLDER_ID' : "1w1g7xvVNKSX2RgHVhuN9UsDB_HLUkLOj", 
    'FR_INPUT_FOLDER_ID' : '1_1Y93e0oljDYYpLHVmPmPJLX6RxXPLb4', 
    'FR_OUTPUT_FOLDER_ID': '16Ytl2momErxkAtcY8WjsJU0mJBk2bIhy', 
    'FICCOMBINE_OUTPUT_FOLDER_ID': '11F8ICSfkRRpFzJtr_feiJA5Lf2FrvaYt', 
    'MASTER_FOLDER_ID': '1JQ17yWOxE5Qs831X1teqi03sFjphFFwk', 
    'OVERWRITE_FOLDER_ID' : '1j9cbFLhxlP4CeH1nfdVlHRNbkpXEwyzi'
}

def get_overwrite_folder_id():
    return id_dict['OVERWRITE_FOLDER_ID']

def get_master_folder_id():
    return id_dict['MASTER_FOLDER_ID']

def get_absa_folder_id(request='both'):
    match request:
        case 'both':
            return id_dict['ABSA_INPUT_FOLDER_ID'], id_dict['ABSA_OUTPUT_FOLDER_ID']
        case 'input':
            return id_dict['ABSA_INPUT_FOLDER_ID']
        case 'output':
            return id_dict['ABSA_OUTPUT_FOLDER_ID']
        case _:
            raise ValueError(f"Unknown specification of request '{request}'")

def get_oasis_folder_id(request='both'):
    match request:
        case 'both':
            return id_dict['OASIS_INPUT_FOLDER_ID'], id_dict['OASIS_OUTPUT_FOLDER_ID']
        case 'input':
            return id_dict['OASIS_INPUT_FOLDER_ID']
        case 'output':
            return id_dict['OASIS_OUTPUT_FOLDER_ID']
        case _:
            raise ValueError(f"Unknown specification of request '{request}'")

def get_contingency_folder_id(request='both'):
    match request:
        case 'both':
            return id_dict['CONTINGENCY_INPUT_FOLDER_ID'], id_dict['CONTINGENCY_OUTPUT_FOLDER_ID']
        case 'input':
            return id_dict['CONTINGENCY_INPUT_FOLDER_ID']
        case 'output':
            return id_dict['CONTINGENCY_OUTPUT_FOLDER_ID']
        case _:
            raise ValueError(f"Unknown specification of request '{request}'")

def get_fr_folder_id(request='both'):
    match request:
        case 'both':
            return id_dict['FR_INPUT_FOLDER_ID'], id_dict['FR_OUTPUT_FOLDER_ID']
        case 'input':
            return id_dict['FR_INPUT_FOLDER_ID']
        case 'output':
            return id_dict['FR_OUTPUT_FOLDER_ID']
        case _:
            raise ValueError(f"Unknown specification of request '{request}'")

def get_ficcombine_output_id():
    return id_dict['FICCOMBINE_OUTPUT_FOLDER_ID']

def get_ficcombine_folder_id(request='all'):
    return (
        get_oasis_folder_id(request='output'),
        get_contingency_folder_id(request='output'),
        get_fr_folder_id(request='output'),
        get_ficcombine_output_id()
    )

def get_all_ids():
    return id_dict

def get_folder_ids(process_type, request='both'):
    match process_type.upper():
        case 'ABSA':
            return get_absa_folder_id(request=request)
        case 'OASIS':
            return get_oasis_folder_id(request=request)
        case 'CONTINGENCY':
            return get_contingency_folder_id(request=request)
        case 'FR':
            return get_fr_folder_id(request=request)
        case 'FICCOMBINE':
            return get_ficcombine_folder_id(request=request)
        case 'OVERWRITE':
            if request != 'both':
                raise ValueError("Overwrite folder only supports 'both' request format")
            return get_overwrite_folder_id()
        case _:
            raise ValueError(f"Unknown process type '{process_type}'")