from googleapiclient.discovery import build
from google.oauth2 import service_account

SCOPES = ["https://www.googleapis.com/auth/drive"]
API_NAME = "drive"
API_VERSION = "v3"
SERVICE_ACCOUNT_FILE = "credentials.json"  # Store credentials securely!

def authenticate_drive():
    creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    return build(API_NAME, API_VERSION, credentials=creds)

#DEBUG
# def general_CSV_search() -> list:
#     try: 
#         service = authenticate_drive()
#         query = "name contains '.csv' and name contains 'ABSA' and name contains 'RF'"
#         page_token = None
#         files = []
#         while True:
#             # pylint: disable=maybe-no-member
#             response = (service.files().list(
#                 q=query, spaces="drive", fields="nextPageToken, files(id, name)", pageToken=page_token,
#                 ).execute())
#             for file in response.get("files", []):
#                 # Process change
#                 print(f'Found file with name: {file.get("name")}, ID: {file.get("id")}')
#             files.extend(response.get("files", []))
#             page_token = response.get("nextPageToken", None)
#             if page_token is None:
#                 break

#     except HttpError as error:
#         print(f"An error occurred: {error}")
#         files = None

#     return files

if __name__ == "__main__":
    #DEBUG
    service = authenticate_drive()
    print(f"Service: {service}") 
    about = service.about().get(fields="user").execute()
    print(f"Connected as: {about['user']['emailAddress']}")
    
    ABSA_INPUT_FOLDER_ID = "1nlYOz8brWYgF3aKsgzpjZFIy1MmmEVxQ"
    ABSA_OUTPUT_FOLDER_ID = "1ELodPGvuV7UZRhTl1x4Phh0PzMDescyG"

    #DEBUG
    input_permissions = service.permissions().list(fileId=ABSA_INPUT_FOLDER_ID).execute()
    output_permissions = service.permissions().list(fileId=ABSA_OUTPUT_FOLDER_ID).execute()
    print(f"Permission to input folder: {input_permissions}")
    print(f"Permission to output folder: {output_permissions}")

    