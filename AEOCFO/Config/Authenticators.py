from googleapiclient.discovery import build
from google.oauth2 import service_account
import os

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

def get_drive_client(key_file):
    creds = service_account.Credentials.from_service_account_file(key_file, scopes=SCOPES["DRIVE"])
    return build(API["NAME"], API["VERSION"], credentials=creds)

def get_bq_credentials(key_file):
    return service_account.Credentials.from_service_account_file(key_file, scopes=SCOPES["BQ"])

def get_googlecloud_credentials(key_file):
    return service_account.Credentials.from_service_account_file(key_file, scopes=SCOPES["GCP"])


def build_account(key_file, platforms=("drive", "bigquery", "googlecloud")):
    credentials = {}

    if "drive" in platforms:
        credentials["drive"] = get_drive_client(key_file)
    if "bigquery" in platforms:
        credentials["bigquery"] = get_bq_credentials(key_file)
    if "googlecloud" in platforms:
        credentials["googlecloud"] = get_googlecloud_credentials(key_file)

    return {
        "key_file": key_file,
        "credentials": credentials,
    }

accounts = {
    "primary": build_account(
        "credentials.json" if IN_CI else ".gcp/credentials.json",
        platforms=("drive", "bigquery", "googlecloud"),
    ),
    "pusher": build_account(
        "ocfo-primary-pusher.json" if IN_CI else ".gcp/ocfo-primary-pusher.json",
        platforms=("drive", "googlecloud"),  # no bigquery for pusher
    ),
}


def authenticate_credentials(acc, platform):
    """
    Retrieve the authenticated credential or client for a given account and platform.

    Parameters:
        acc (str): The name of the account (e.g., "primary", "pusher").
        platform (str): The service platform (e.g., "drive", "bigquery", "googlecloud").

    Returns:
        Authenticated credentials or API client object.
    """
    acc = acc.strip().lower()
    platform = platform.strip().lower()

    if acc not in accounts:
        raise ValueError(f"Account '{acc}' not supported. Choose from: {list(accounts.keys())}")
    
    supported_plats = accounts[acc].get("credentials", {})
    if platform not in supported_plats:
        raise ValueError(f"Platform '{platform}' not supported for account '{acc}'. Supported: {list(supported_plats.keys())}")
    
    return supported_plats[platform]
