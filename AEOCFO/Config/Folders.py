misc_ids = {
    'MASTER_FOLDER_ID': "1JQ17yWOxE5Qs831X1teqi03sFjphFFwk",
    'OVERWRITE_FOLDER_ID': "1j9cbFLhxlP4CeH1nfdVlHRNbkpXEwyzi", 
    'OVERWRITE_DATASET_ID' : "OVERWRITE", 
    'OVERWRITE_BUCKET_ID' : ""
}

id_dict = {
    'ABSA': {
        'input': "1nlYOz8brWYgF3aKsgzpjZFIy1MmmEVxQ",
        'output': "1ELodPGvuV7UZRhTl1x4Phh0PzMDescyG",
        'datasetid' : "ABSA", 
        'test': {
            'test_file_names': [],
            'input': "1nlYOz8brWYgF3aKsgzpjZFIy1MmmEVxQ", 
            'output' : "1U6Z2DnMR1YKNO2jaIS61tLB50Yujt0dO", 
            'test_bq_dataset' : 'test'
        }
    },
    'OASIS': {
        'input': "1WwgSJoEyz1RKsdSJop3oSDlZ5LqZ_ZKz",
        'output': "1r-1xZMnqYkp1BS59Mnq4Gs3z0yKeUAIL",
        'datasetid': "OASIS", 
        'test': {
            'test_file_names': [],
            'input': "1WwgSJoEyz1RKsdSJop3oSDlZ5LqZ_ZKz", 
            'output' : "1tNc-Q-7I3KNoEpb6P5LhIjhoChX_wDBd", 
            'test_bq_dataset' : 'test'
        }
    },
    'CONTINGENCY': {
        'input': "1pNoRbJgNeVacqr4RzTbYCi1DVZ1qiwzP",
        'output': "1w1g7xvVNKSX2RgHVhuN9UsDB_HLUkLOj",
        'datasetid': "CONTINGENCY",  
        'test': {
            'test_file_names': [],
            'input': "1pNoRbJgNeVacqr4RzTbYCi1DVZ1qiwzP", 
            'output' : "1aygKPoPOQPbWvhjZTSKtF9Je9I6xgrLv", 
            'test_bq_dataset' : 'test'
        }
    },
    'FR': {
        'input': "1_1Y93e0oljDYYpLHVmPmPJLX6RxXPLb4",
        'output': "16Ytl2momErxkAtcY8WjsJU0mJBk2bIhy", 
        'datasetid': "FR", 
        'test': {
            'test_file_names': ['S09', 'S10', 'S11'],
            'input': "1_1Y93e0oljDYYpLHVmPmPJLX6RxXPLb4", 
            'output' : "1BDMsXPXDjTQTaU3zshmlQlQfp0kd3XWS", 
            'test_bq_dataset' : 'test'
        }
    },
    'FICCOMBINE': {
        'output': "11F8ICSfkRRpFzJtr_feiJA5Lf2FrvaYt", 
        'datasetid': 'FICCOMBINE', 
        'test': {
            'test_file_names': [],
            'input': "", 
            'output' : "", 
            'test_bq_dataset' : 'test'
        }
    },
    'ACCOUNTS': {
        'input': "", 
        'output': "", 
        'datasetid': 'ACCOUNTS', 
        'test': {
            'test_file_names': [],
            'input': "", 
            'output' : "", 
            'test_bq_dataset' : 'test'
        }
    }, 
    'TRANSACS': {
        'input': "", 
        'output': "", 
        'datasetid': 'TRANSACS', 
        'test': {
            'test_file_names': [],
            'input': "", 
            'output' : "", 
            'test_bq_dataset' : 'test'
        }
    }
}

def get_all_ids():
    return id_dict

def get_overwrite_folder_id():
    return misc_ids['OVERWRITE_FOLDER_ID']

def get_overwrite_dataset_id():
    return misc_ids['OVERWRITE_DATASET_ID']

def get_overwrite_bucket_id():
    return misc_ids['OVERWRITE_BUCKET_ID']

def get_master_folder_id():
    return misc_ids['MASTER_FOLDER_ID']

def get_folder_id(process, request='both', testing=False):
    process = process.upper()
    if process not in id_dict:
        raise ValueError(f"Unknown process type '{process}'")

    process_info = id_dict[process]

    if testing:
        process_info = process_info.get('test', {})

    match request:
        case 'both':
            return process_info.get('input'), process_info.get('output')
        case 'input':
            return process_info.get('input')
        case 'output':
            return process_info.get('output')
        case _:
            raise ValueError(f"Unknown request type '{request}'")
        
def get_test_file_names(process):
    process = process.upper()
    if process not in id_dict:
        raise ValueError(f"Unknown process type '{process}'")
    
    return id_dict[process].get('test').get('test_file_names', [])

def get_dataset_ids(process_type, testing=False):
    process = process_type.upper()
    if process not in id_dict:
        raise ValueError(f"Unknown process type '{process_type}'")
    
    if testing:
        return id_dict[process].get('test').get('test_bq_dataset')
    
    return id_dict[process].get('datasetid')
        
def get_ficcombine_folder_id(request='all', testing=False):
    if testing:
        return id_dict['FICCOMBINE']['test']
    return (
        get_folder_id('OASIS', request='output'),
        get_folder_id('CONTINGENCY', request='output'),
        get_folder_id('FR', request='output'),
        id_dict['FICCOMBINE']['output']
    )