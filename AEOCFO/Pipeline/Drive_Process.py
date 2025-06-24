from AEOCFO.Utility.Logger_Utils import get_logger
from AEOCFO.Transform.Processor import ASUCProcessor
from AEOCFO.Extract.Drive_Pull import drive_pull
from AEOCFO.Load.Drive_Push import drive_push
from AEOCFO.Config.Folders import get_folder_ids

def drive_process(directory_ids: dict[str, str | list[str]], process_type: str, blind_to = None, duplicate_handling: str = "Ignore", year: str | None = None, reporting: bool = False, debug: bool = False) -> None:
    """
    Handles the entire extract, transform and load process given an input and output dir id. Assumes implementation of an _authenticate() func to initiate service account.
    directories: directory with two keys, 'input' and 'output' and corresponding values being either strings or tuples of strings listing out input and output directory ids

    TO DO
    - figure out how to modify functions to return names so we can automatically name files
    - duplicate naming scheme
    """
    # dataframes: dict[str : pd.DataFrame]
    # raw_names: list[str]
    # --> go into ASUC Processor, which outputs --> 
    # cleaned_dfs: list[pd.DataFrame]
    # cleaned_names: list[str]  
    # --> go into upload func
    logger = get_logger(process_type)
    logger.info(f"--- START DRIVE PROCESSING: '{process_type}' ---")

    assert 'input' in directory_ids.keys() and 'output' in directory_ids.keys(), f"inputed diction of directory ids malformed, no 'input' and 'output' keys"

    if process_type != 'FICCOMBINE':
        in_dir_id, out_dir_id = directory_ids['input'], directory_ids['output']
        assert isinstance(in_dir_id, str), f"input directory ID is not a string: {in_dir_id}"
        assert isinstance(out_dir_id, str), f"output directory ID is not a string: {out_dir_id}"

        dataframes, raw_names = drive_pull(in_dir_id, process_type=process_type, reporting=reporting, debug=debug)
        if dataframes == {} and raw_names == []:
            logger.info(f"No files of query type {process_type} found in designated folder ID{in_dir_id}")
            return
        
        logger.info(f"--- START: {process_type} ASUCProcessor ---")
        processor = ASUCProcessor(process_type)       
        cleaned_dfs, cleaned_names = processor(dataframes, raw_names, reporting=reporting)
        processing_type = processor.get_type()
        logger.info(f"ASUCProcessor successfully complete!")
        logger.info(f"--- END: {process_type} ASUCProcessor ---")

        df_ids: dict[str : str] = drive_push(out_dir_id, cleaned_dfs, cleaned_names, processing_type, blind_to=blind_to, duplicate_handling=duplicate_handling, reporting=reporting)
    elif process_type == 'FICCOMBINE':
        if year is None:
            raise ValueError("Year must be provided for FICOMM_COMBINED processing")
        
        inputs = directory_ids['input']
        if not (isinstance(inputs, (list, tuple)) and len(inputs) == 3):
            raise ValueError("For 'FICCOMBINE', 'input' must be a list or tuple of 3 directory IDs (OASIS, CONTINGENCY, FR)")

        OASIS_ID, CONTINGENCY_ID, FR_ID = inputs
        FICCOMBINE_ID = directory_ids['output']
        for dir_id, name in zip([OASIS_ID, CONTINGENCY_ID, FR_ID, FICCOMBINE_ID], ["OASIS", "CONTINGENCY", "FR", "FICCOMBINE"]):
            assert isinstance(dir_id, str), f"{name} directory ID is not a string: {dir_id}"
        
        oasis_dict, oasis_names_dict = drive_pull(OASIS_ID, process_type=process_type, name_keywords=[year], reporting=reporting)
        contingency_dict, contingency_names_dict = drive_pull(CONTINGENCY_ID, process_type=process_type, name_keywords=[year], reporting=reporting)
        fr_dict, fr_names_dict = drive_pull(FR_ID, process_type=process_type, name_keywords=[year], reporting=reporting)

        assert len(oasis_dict) != 0, f"No OASIS files for year {year} found"
        assert len(contingency_dict) != 0, f"No Ficomm-Cont files for year {year} found"
        assert len(fr_dict) != 0, f"No FR files for year {year} found"

        processor = ASUCProcessor(process_type)

        merged_outputs, merged_names = processor.ficomm_merge( # we use the function directy rather than relying on a __call__method --> although can implement dictionary unpacking method 
            oasis_dict=oasis_dict,
            fr_dict=fr_dict,
            contingency_dict=contingency_dict,
            fr_names_dict=fr_names_dict,
            contingency_names_dict=contingency_names_dict,
            year=year,
            reporting=reporting
        )

        df_ids = drive_push(FICCOMBINE_ID, merged_outputs, merged_names, process_type, blind_to=blind_to, duplicate_handling=duplicate_handling, reporting=reporting)

    logger.info(f"--- END DRIVE PROCESSING: '{process_type}' ---")