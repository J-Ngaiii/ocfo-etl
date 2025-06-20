from googleapiclient.discovery import build
from google.oauth2 import service_account

SCOPES = ["https://www.googleapis.com/auth/drive"]
API_NAME = "drive"
API_VERSION = "v3"
SERVICE_ACCOUNT_FILE = "credentials.json"  # Store credentials securely!



# Setup 
def authenticate_drive():
    creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    return build(API_NAME, API_VERSION, credentials=creds)