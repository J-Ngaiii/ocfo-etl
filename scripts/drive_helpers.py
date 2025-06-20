def get_unique_name_in_folder(service, archive_folder_id, base_name) -> str:
    """
    Returns a unique name in the archive folder by checking for existing files and appending (1), (2), etc.
    """
    query = f"'{archive_folder_id}' in parents and trashed = false"
    response = service.files().list(q=query, fields="files(name)").execute()
    existing_names = set(file['name'] for file in response.get('files', []))

    if base_name not in existing_names:
        return base_name

    # Find next available numbered name
    counter = 1
    while f"{base_name} ({counter})" in existing_names:
        counter += 1
    return f"{base_name} ({counter})"