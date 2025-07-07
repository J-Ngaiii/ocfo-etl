# ocfo-etl
Repo hosting the ETL scripts for the OCFO Primary Database system. 

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

# Setting up this repository on your local machine
First create a conda enviornment. Run:
- `conda create -n ocfo-etl python=3.11.8`
- `conda activate ocfo-etl`
    - Check that the python version is 3.11.8: `conda list python`

Then install dependencies. Make sure you're in the root of the repo with the `requirments.txt` file in your local directory first. 
- You can check your current working directory with `pwd` 
- You can check if `requirements.txt` exists in your current working directory with `ls`

The simplest solution is just to run `pip install -r requirements.txt`. However, this may not work with all machines (especially macbooks).

If installing completely from requirements doesn't work try:
- `conda install numpy pandas scikit-learn pyarrow`
- `pip install -r requirements.txt`

After you've installed all requirements you need to install the AEOCFO package outlined in this repository. To do that run:
- `pip install -e .`

# Running Transformations
There are 4 functional main scripts that run the processing pipeline for 4 main datasets: ABSA, OASIS, FR and Contingency. These scripts are in AEOCFO/Pipeline. To run the ABSA pipeline for example move to the root of the repo then run: 
- `AEOCFO/Pipeline/ABSA.py`

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

# Mechanisms
## Lifecycle of a raw file
This ETL pipeline expects that any and all raw files are ingested through the relevant google drive folders. Currently there is no handling for specifying google drive folders, rather there are designated input and output folders for each of the 4 main datasets and their ids are hardcoded into the Folders.py file in AEOCFO/Config/.

There is a Execute.py script in AEOCFO/Pipeline/. This is the primary script in charge of executing the entire ETL workflow. Given a dataset type to process (ABSA, OASIS, FR or Contingency), this script will pull functions from the relevant Extract/, Transform/ and Load/ directories (all within AEOCFO/) and execute the full workflow. This involves:
- Pulling files from the designated Google Drive folders holding the raw files for that particular dataset type (Extract/Drive_Pull.py)
- Cleaning those files (The entire Transform/ folder and the Transform/Processor.py file)
- Pushing those files to designated 'clean' folders in Google Drive (Load/Drive_Push.py)
- Pulling those same (now cleaned) files again from the same designated 'clean' folders in Google Drive (Extract/Drive_Pull.py)
- Converting those into tables and pushing them to BigQuery dataset objects corresponding with the appropriate dataset types (ABSA, OASIS, FR or Contingency)

Named excution scripts like `ABSA.py` or `Contingency.py` import and use the `main` function from `Execute.py`.

You can specify toggling on or off prints/logs as well as turn off the entire BigQuery or Drive components of the pipeline with flags when you execute the respective files: 
- `python ABSA.py --no-verbose`
- `python ABSA.py --no-bigquery`
- `python ABSA.py --no-drive`

There is also a testing mode that will only select certain test files and output the results of executing the ETL workflow on those test files. The outputs are storred in a google drive test outputs folder. The name of designated test files as well as the folders from which the workflow pulls test files from and pushes cleaned test fils to are all defined in the Folders.py file under Config/. Initiate testing mode with flags. 
- `python ABSA.py --testing`

Finally there is an `Any.py` file under Pipeline/ that is just a out of box implementation of the `main` function under `Execute.py`. Running `Any.py` allows you to specify any dataset you want to process. Examples include:  
- `python Any.py --dataset 'ABSA' --no-verbose --testing`
- `python Any.py --dataset 'Contingency' --no-drive --testing`
- `python Any.py --dataset 'FR' --no-bigquery`

## Naming
- Some kinds of processing rely on finding certain pieces of information in the raw file name when the intake raw files for processing (eg year and date for OASIS, FR, ABSA)
- If the needed information is missing from teh raw file name, the transformation pipelien will still try to proces it but append 'MISMATCH-" to the beginning of the file name
- If the loading pipeline is uploading a file to a folder that shares the same name as a file already in said folder, there are 3 modes currently implemented to handle that:
    - Add a number to the file name so its unique
    - Skip uploading the newly processed file with the duplicate name
    - Overwrite the file in the folder with the newly processed file