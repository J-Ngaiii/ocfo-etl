from AEOCFO.Utility.Drive_Helpers import download_csv, download_text, download_any_spreadsheet

PROCESS_CONFIG = {
    'ABSA': {
        'query_type': 'csv',
        'handler': lambda fid, mime, svc: download_csv(fid, svc)
    },
    'OASIS': {
        'query_type': 'csv',
        'handler': lambda fid, mime, svc: download_csv(fid, svc)
    },
    'CONTINGENCY': {
        'query_type': 'gdoc',
        'handler': lambda fid, mime, svc: download_text(fid, mime, svc)
    },
    'FR' : {
        'query_type': 'csv+gspreadsheet',
        'handler': lambda fid, mime, svc: download_any_spreadsheet(fid, mime, svc, output='both')
    }, 
    'BIGQUERY' : {
        'query_type': 'csv', 
        'handler': lambda fid, mime, svc: download_csv(fid, svc)
    }, 
    'FICCOMBINE' : {
        'query_type': 'csv', 
        'handler': lambda fid, mime, svc: download_csv(fid, svc)
    }, 
    'ACCOUNTS' : {
        'query_type': 'csv', 
        'handler': lambda fid, mime, svc: download_csv(fid, svc)
    }, 
    'TRANSACS' : {
        'query_type': 'csv', 
        'handler': lambda fid, mime, svc: download_csv(fid, svc)
    }
}

def get_process_config():
    return PROCESS_CONFIG