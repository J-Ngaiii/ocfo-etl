from AEOCFO.Config.Folders import *
from AEOCFO.Config.Authenticators import authenticate_credentials

if __name__ == "__main__":
    service = authenticate_credentials(acc='primary')
    about = service.about().get(fields="user").execute()
    print(f"Connected as: {about['user']['emailAddress']}")

    folder_dicts = get_all_ids()
    for key, folder_dictionary in folder_dicts.items():
        input_folder = folder_dictionary.get('input')
        output_folder = folder_dictionary.get('output')
        try:
            folder = service.files().get(fileId=input_folder, fields='id, name').execute()
            print(f"Permission to {key} input folder: Accessible! (Folder name: {folder['name']})")
        except Exception as e:
            print(f"Permission to {key} input folder: DENIED or ERROR! ({e})")
        try:
            folder = service.files().get(fileId=output_folder, fields='id, name').execute()
            print(f"Permission to {key} output folder: Accessible! (Folder name: {folder['name']})")
        except Exception as e:
            print(f"Permission to {key} output folder: DENIED or ERROR! ({e})")

    