# ocfo-etl
Repo hosting the ETL scripts for the OCFO Primary Database system. 

# Repository Structure
- AEOCFO: Almighty Repo containing main body of code
    - Config
        - Drive_Config.py: Configuration for all Google Drive API files
        - Folders.py: Folder IDs for all relevant Google Drive folders
    - Extract
        - Drive_Pull.py: Extraction script for pulling files from drive folders
    - Load
        - Drive_Push.py: Loading script for pushing files into drive folders
    - Pipeline
        - Drive_Process.py: Main processing script for executing ETL on google drive files. Combines functions from Drive_Pull.py and Drive_Push.py
        - ABSA.py: Script that implements Drive_Process.py to clean and load raw ABSA files in the OCFO Primary Database google drive folders
        - Contingency.py: Script that implements Drive_Process.py to clean and load raw Finance Committee Agenda google docs in the OCFO Primary Database google drive folders to extract Contingency Funding data
        - OASIS.py: Script that implements Drive_Process.py to clean and load raw OASIS club registration spreadsheets in the OCFO Primary Database google drive folders
        - FR.py: Script that implements Drive_Process.py to clean and load raw FR spreadsheets in the OCFO Primary Database google drive folders
    - Transform: Copy of ASUCExplore package
        - ABSA_Processor.py: Contains processing functions for processing ABSA spreadsheets, outputting a dataframe to be uploaded by Drive_Push
        - Agenda_Processor.py: Contains processing functions for processing Finance Committee Agendas, outputting a dataframe to be uploaded by Drive_Push
        - OASIS_Processor.py: Contains processing functions for processing OASIS spreadsheets, outputting a dataframe to be uploaded by Drive_Push
        - FR_Processor.py: Contains processing functions for processing FR spreadsheets, outputting a dataframe to be uploaded by Drive_Push
        - Processory.py: Contains **ASUCProcessor class object** that implements all the processing functions from ABSA_Processor.py, Agenda_Processor.py, OASIS_Processor.py, etc to take in files from Drive_Pull.py --> clean them using the processing functions--> upload cleaned files using Drive_Push.py
    - Utility
        - Authenticators.py: Authenticators for Google Drive API scripts
        - Drive_Helpers.py: Helper functions for Drive_Pull.py and Drive_Push.py
        - Cleaning.py: Cleaning functions
        - Utils.py: Utility functions
- debugs: Scripts for debugging
- legacy: Old scripts

# Requirements
## APIs:
- Google Drive API
## Python:
- ASUC Explore requires version > 3.11 but < 3.12, generaly recommend 3.11.8
## Libraries:
- Check requirements.txt

# Setting Up Google Drive API and Service Account
- Go to Google Cloud Console
- Search up and enable Google Drive API
- Go to Service Accounts and make a service account -> give it editor permissions
- Open up your new service account -> go to keys -> create new key JSON format -> should automatically download  