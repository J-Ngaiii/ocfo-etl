id_dict = {
    'ABSA_DATASET_ID' : "ABSA", 
    'CONTINGENCY_DATASET_ID' : "CONTINGENCY", 
    'OASIS_DATASET_ID' : "OASIS", 
    'FR_DATASET_ID' : "FR", 
    'OVERWRITE_DATASET_ID' : "OVERWRITE", 
    'FICCOMBINE_DATASET_ID' : 'FICCOMBINE'
}

def get_overwrite_dataset_id():
    return id_dict['OVERWRITE_DATASET_ID']

def get_absa_dataset_id():
    return id_dict['ABSA_DATASET_ID']

def get_contingency_dataset_id():
    return id_dict['CONTINGENCY_DATASET_ID']

def get_oasis_dataset_id():
    return id_dict['OASIS_DATASET_ID']

def get_fr_dataset_id():
    return id_dict['FR_DATASET_ID']

def get_ficcombine_dataset_id():
    return id_dict['FICCOMBINE_DATASET_ID']

def get_dataset_ids(process_type):
    match process_type.upper():
        case 'ABSA':
            return get_absa_dataset_id()
        case 'OASIS':
            return get_oasis_dataset_id()
        case 'CONTINGENCY':
            return get_contingency_dataset_id()
        case 'FR':
            return get_fr_dataset_id()
        case 'FICCOMBINE':
            return get_ficcombine_dataset_id()
        case 'OVERWRITE':
            return get_overwrite_dataset_id()
        case _:
            raise ValueError(f"Unknown process type '{process_type}'")