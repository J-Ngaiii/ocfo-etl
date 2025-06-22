from AEOCFO.Config.Folders import *
from AEOCFO.Utility.Authenticators import authenticate_drive

if __name__ == "__main__":
    service = authenticate_drive()
    about = service.about().get(fields="user").execute()
    print(f"Connected as: {about['user']['emailAddress']}")

    folder_ids = get_all_ids()
    for key, folder_id in folder_ids.items():
        try:
            folder = service.files().get(fileId=folder_id, fields='id, name').execute()
            print(f"Permission to {key} folder: Accessible! (Folder name: {folder['name']})")
        except Exception as e:
            print(f"Permission to {key} folder: DENIED or ERROR! ({e})")

    