from googleapiclient.discovery import build
from google.oauth2 import service_account
import os

#NOTE
# Removing function calls from datastructures and dictionaries
# makes it so that service_account.Credentials.from_service_account_file(key_file) doesn't get invoked at import time 
# then github doesn't invoke a credentials.json doesn't exist error
IN_CI = os.getenv("GITHUB_ACTIONS") == "true"

SCOPES = {
    "DRIVE": ["https://www.googleapis.com/auth/drive"],
    "BQ": ["https://www.googleapis.com/auth/cloud-platform"],
    "GCP": ["https://www.googleapis.com/auth/cloud-platform"],
}

API = {
    "NAME": "drive",
    "VERSION": "v3",
}

# Store file paths and platforms here — no credential objects yet
accounts_info = {
    "primary": {
        "key_file": "credentials.json" if IN_CI else ".gcp/credentials.json",
        "platforms": ("drive", "bigquery", "googlecloud"),
    },
    "pusher": {
        "key_file": "ocfo-primary-pusher.json" if IN_CI else ".gcp/ocfo-primary-pusher.json",
        "platforms": ("drive", "googlecloud"),
    },
}

def get_drive_client(key_file):
    creds = service_account.Credentials.from_service_account_file(key_file, scopes=SCOPES["DRIVE"])
    return build(API["NAME"], API["VERSION"], credentials=creds)

def get_bq_credentials(key_file):
    return service_account.Credentials.from_service_account_file(key_file, scopes=SCOPES["BQ"])

def get_googlecloud_credentials(key_file):
    return service_account.Credentials.from_service_account_file(key_file, scopes=SCOPES["GCP"])

def authenticate_credentials(acc, platform):
    acc = acc.strip().lower()
    platform = platform.strip().lower()

    if acc not in accounts_info:
        raise ValueError(f"Account '{acc}' not supported. Choose from: {list(accounts_info.keys())}")

    info = accounts_info[acc]
    key_file = info["key_file"]
    platforms = info["platforms"]

    if platform not in platforms:
        raise ValueError(f"Platform '{platform}' not supported for account '{acc}'. Supported: {list(platforms)}")

    # Instantiate on demand 
    if platform == "drive":
        return get_drive_client(key_file)
    elif platform == "bigquery":
        return get_bq_credentials(key_file)
    elif platform == "googlecloud":
        return get_googlecloud_credentials(key_file)
    else:
        raise ValueError(f"Unknown platform '{platform}' requested.")

