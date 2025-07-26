from AEOCFO.Config.Folders import *
from AEOCFO.Config.Authenticators import authenticate_credentials

def check_account(account):
    service = authenticate_credentials(acc=account, platform='drive')
    about = service.about().get(fields="user").execute()
    print(f"\nConnected as: {about['user']['emailAddress']}")

    folder_dicts = get_all_ids()
    for key, folder_dictionary in folder_dicts.items():
        input_folder = folder_dictionary.get('input')
        output_folder = folder_dictionary.get('output')
        skip = False

        if input_folder is None: 
            print(f"No registered input folder for {key}")
            skip = True
        if output_folder is None:
            print(f"No registered output folder for {key}")
            skip = True
        if skip:
            continue

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

if __name__ == "__main__":
    accounts = ['primary', 'pusher']
    for acc in accounts:
        check_account(acc)

    