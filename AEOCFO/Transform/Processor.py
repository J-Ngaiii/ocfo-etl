import pandas as pd
from typing import Callable
import re
from AEOCFO.Utility.Cleaning import is_type
from AEOCFO.Transform import ABSA_Processor, Agenda_Processor, OASIS_Abridged

class ASUCProcessor:
    """Wrapper class for processors. Specify the file type (eg. ABSA) then the __call__ method executes the appropriate processing function, outputting the result.
    The get_type method also outputs the type of processing (eg. ABSA processing pipeline) the ASUCProcessor instance was instructed to execute. 
    Both the actual output of processing (list of processed pd.DataFrame objects) and the type of processing initiated (self.type) are returned to an upload function. 
    
    Processing functions must take in:
    - df_dict (dict[str:pd.DataFrame]): dictionary where keys are file ids and values are the raw files converted into pandas dataframes.
    - reporting (str): parameter that tells the processing function whether or not to print outputs on processing progress.
    - names (dict[str:str]): dictionary where keys are file ids and values are raw file names.
    
    Processing functions must return:
    - list of processed pd.DataFrame objects
    - list of names with those failing naming conventions highighted
        - highlighting means we append 'MISMATCH' to the beginning the name
    
    Higher level architecture:
    - drive_pull func --> outputs raw files as dataframes and list of raw file names
    - ASUCProcessor instance 
        - takes in list of raw files names and raw fils as dataframes

        --> outputs processed fils in a list, type of processing executed and refined list of names with naming convention mismatches flagged
    - drive_push func:
        - From ASUCProcessor instance: take in the outputs of the processed files, the type of processing executed and updated list of names
        
        --> adjust the names of the files accodingly to indicate they're cleaned (based on raw file name and type of processing initiated) then upload files back into ocfo.database drive.

    Dependencies:
    - Currently depends on having ABSA_Processor from ASUCExplore > Core > ABSA_Processor.py alr imported into the file
    """
    tagging_convention = {
        "ABSA" : ("RF", "GF"), # ABSA processing outputs changes the "RF" raw file classification to the 'GF' general file classification, we don't need to tell the upload func to name the file ABSA because the raw fill should alr be named ABSA
        "Contingency" : ("RF", "GF"), 
        "OASIS" : ("RF", "GF")
    }

    name_convention = {
        "ABSA" : "ABSA", # designates what the name of the cleaned file should be called
        "Contingency" : "Ficomm-Cont", 
        "OASIS" : "OASIS"
    }

    raw_name_dependency = { # for some files we need to check what they're named as, for others we don't
        "ABSA" : True, 
        "Contingency" : False,
        "OASIS" : True
    }

    processing_func = { # this is just for record
        "ABSA" : ABSA_Processor, 
        "Contingency" : Agenda_Processor,
        "OASIS" : OASIS_Abridged
    }

    def __init__(self, type: str):
        """self.type currently handles for 'ABSA', 'OASIS' and 'Contingency'."""
        self.type = type
        self.processors = {
            "ABSA": self.absa,
            "Contingency": self.contingency, 
            "OASIS": self.oasis
        }

    @classmethod
    def get_tagging(self) -> dict[str:str]:
        return ASUCProcessor.tagging_convention
    
    @classmethod
    def get_name(self) -> dict[str:str]:
        return ASUCProcessor.name_convention
    
    def get_type(self) -> str:
        return self.type

    def get_processing_func(self) -> Callable:
        return self.processing_func[self.get_type()]

    def absa(self, df_dict, names, reporting = False) -> list[pd.DataFrame]:
        # need to check if df_dict and names are the same length but handle for case when name is a single string
        assert isinstance(df_dict, dict), f"df_dict is not a dictionary but {type(df_dict)}"
        assert is_type(list(df_dict.keys()), str), f"df_dict keys are not all strings"
        assert is_type(list(df_dict.values()), pd.DataFrame), f"df_dict values are not all pandas dataframes"

        assert isinstance(names, dict), f"names is not a dictionary but {type(names)}"
        assert is_type(list(names.keys()), str), f"names keys are not all strings"
        assert is_type(list(names.values()), str), f"names values are not strings"

        assert len(df_dict) == len(names), f"Given {len(df_dict)} dataframe(s) but {len(names)} name(s)"

        if not df_dict:
            raise ValueError("df_dict is empty! No DataFrames to process.")
        if not names:
            raise ValueError("names is empty! No file names to process.")
        
        df_lst = list(df_dict.values())
        id_lst = list(df_dict.keys())
        name_lst = list(names.values())

        rv = []
        for i in range(len(df_lst)):
            try: 
                df = df_lst[i]
                id = id_lst[i]
                name = name_lst[i]
                year = re.search(r'(?:FY\d{2}|fr\d{2}|\d{2}\-\d{2}\|\d{4}\-\d{4}\))', name)[0]
                name_lst[i] = f"{self.get_name()['ABSA']}-{year}-{self.get_tagging()['ABSA'][0]}" # Contingency draws from ficomm files formatted "Ficomm-date-RF"
                if self.get_type().lower() not in name.lower():
                    print(f"File does not matching processing naming conventions!\nFile name: {name}\nID: {id}") # do we raise to stop program or just print?
                    name_lst[i] = 'MISMATCH-' + name_lst[i] # WARNING: mutating array as we loop thru it, be careful
                try:
                    processing_function = self.get_processing_func()
                except Exception as e:
                    print(f"Processing function {self.get_processing_func().__name__} errored while processing file {name}, ID {id}\nPassing error from {self.get_processing_func().__name__}")
                    raise e
                rv.append(processing_function(df))
                if reporting:
                    print(f"Successfully ran {self.get_processing_func().__name__} on File: {name}, id: {id}")
            except Exception as e:
                raise e
        return rv, name_lst
    
    def contingency(self, txt_dict, names, reporting = False) -> list[pd.DataFrame]:
        """
        Function that takes in a dictionary of txt files and names then outputs a dictionary of processed txt files with updated names. 
        Date is appended to updated file names under formatting: %m/%d/%Y.
        """
        assert isinstance(txt_dict, dict), f"df_dict is not a dictionary but {type(txt_dict)}"
        assert is_type(list(txt_dict.keys()), str), f"df_dict keys are not all strings"
        assert is_type(list(txt_dict.values()), str), f"df_dict values are not all strings"

        assert isinstance(names, dict), f"names is not a dictionary but {type(names)}"
        assert is_type(list(names.keys()), str), f"names keys are not all strings"
        assert is_type(list(names.values()), str), f"names values are not strings"

        assert len(txt_dict) == len(names), f"Given {len(txt_dict)} dataframe(s) but {len(names)} name(s)"

        if not txt_dict:
            raise ValueError("df_dict is empty! No DataFrames to process.")
        if not names:
            raise ValueError("names is empty! No file names to process.")
        
        txt_lst = list(txt_dict.values())
        id_lst = list(txt_dict.keys())
        name_lst = list(names.values())

        rv = []
        for i in range(len(txt_lst)):
            try: 
                txt = txt_lst[i]
                id = id_lst[i]
                name = name_lst[i]
                
                processing_function = self.get_processing_func()
                try:
                    output, date = processing_function(txt,  debug=False)
                except Exception as e:
                    print(f"Processing function {self.get_processing_func().__name__} errored while processing file {name}, ID {id}\nPassing error from {self.get_processing_func().__name__}")
                    raise e
                date_formatted = pd.Timestamp(date).strftime("%m/%d/%Y")
                rv.append(output)
                # HARDCODE ALERT
                name_lst[i] = f"{self.get_name()['Contingency']}-{date_formatted}-{self.get_tagging()['Contingency'][0]}" # Contingency draws from ficomm files formatted "Ficomm-date-RF"
                if 'ficomm' not in name.lower() and 'finance committee' not in name.lower():
                    print(f"File does not matching processing naming conventions!\nFile name: {name}\nID: {id}") # do we raise to stop program or just print?
                    name_lst[i] = 'MISMATCH-' + name_lst[i] # WARNING: mutating array as we loop thru it, be careful
                if reporting:
                    print(f"Successfully ran {self.get_processing_func().__name__} on File: {name}, id: {id}")
            except Exception as e:
                raise e
        return rv, name_lst
    
    def oasis(self, df_dict, names, reporting = False) -> list[pd.DataFrame]:
        assert isinstance(df_dict, dict), f"df_dict is not a dictionary but {type(df_dict)}"
        assert is_type(list(df_dict.keys()), str), f"df_dict keys are not all strings"
        assert is_type(list(df_dict.values()), pd.DataFrame), f"df_dict values are not all pandas dataframes"

        assert isinstance(names, dict), f"names is not a dictionary but {type(names)}"
        assert is_type(list(names.keys()), str), f"names keys are not all strings"
        assert is_type(list(names.values()), str), f"names values are not strings"

        assert len(df_dict) == len(names), f"Given {len(df_dict)} dataframe(s) but {len(names)} name(s)"

        if not df_dict:
            raise ValueError("df_dict is empty! No DataFrames to process.")
        if not names:
            raise ValueError("names is empty! No file names to process.")
        
        df_lst = list(df_dict.values())
        id_lst = list(df_dict.keys())
        name_lst = list(names.values())

        rv = []
        for i in range(len(df_lst)):
            try: 
                df = df_lst[i]
                id = id_lst[i]
                name = name_lst[i]
                year = re.search(r'(?:FY\d{2}|fr\d{2}|\d{2}\-\d{2}\|\d{4}\-\d{4}\))', name)[0]
                name_lst[i] = f"{self.get_name()['OASIS']}-{year}-{self.get_tagging()['OASIS'][0]}" # OASIS draws from ficomm files formatted "OASIS-date-RF"
                if self.get_type().lower() not in name.lower():
                    print(f"File does not matching processing naming conventions!\nFile name: {name}\nID: {id}") # do we raise to stop program or just print?
                    name_lst[i] = 'MISMATCH-' + name_lst[i] # WARNING: mutating array as we loop thru it, be careful
                processing_function = self.get_processing_func()
                try:
                    rv.append(processing_function(df, year))
                except Exception as e:
                    print(f"Processing function {self.get_processing_func().__name__} errored while processing file {name}, ID {id}\nPassing error from {self.get_processing_func().__name__}")
                    raise e
                if reporting:
                    print(f"Successfully ran {self.get_processing_func().__name__} on File: {name}, id: {id}")
            except Exception as e:
                raise e
        return rv, name_lst
        
    # A little inspo from CS189 HW6
    def __call__(self, df_dict: dict[str, pd.DataFrame], names: dict[str, str], reporting: bool = False) -> list[pd.DataFrame]:
        """Call the appropriate processing function based on type."""
        if self.type not in self.processors:
            raise ValueError(f"Unsupported processing type '{self.type}'")
        return self.processors[self.type](df_dict, names, reporting) 