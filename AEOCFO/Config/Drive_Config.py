from AEOCFO.Utility.Drive_Helpers import download_csv, download_text

PROCESS_CONFIG = {
    'ABSA': {
        'query_type': 'csv',
        'handler': lambda fid, mime, svc: download_csv(fid, svc)
    },
    'OASIS': {
        'query_type': 'csv',
        'handler': lambda fid, mime, svc: download_csv(fid, svc)
    },
    'Contingency': {
        'query_type': 'gdoc',
        'handler': lambda fid, mime, svc: download_text(fid, mime, svc)
    },
}

def get_process_config():
    return PROCESS_CONFIG