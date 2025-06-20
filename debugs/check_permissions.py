from AEOCFO.Config.Folders import *
from AEOCFO.Utility.Authenticators import authenticate_drive

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

    #DEBUG
    for key, value in get_all_ids().items():
        print(f"Permission to {key} folder: Accessible!")

    