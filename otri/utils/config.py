import json
from pathlib import Path

open_files = dict()

def get_value(key: str, filename: str = "config"):
    '''
    Reads config file and looks for the given key.

    Parameters:
        key : str
            Requested configuration key.
        filename : str
            Name of a json file. Must not contain the file extension.
    Returns:
        str containing the value if the key was found in the given config file, None otherwise.
    '''
    if(open_files.get(filename, None) == None):
        try:
            open_files[filename] = Path("{}.json".format(filename)).open("r")
        except FileNotFoundError:
            return None
    
    with open_files[filename] as config_file:
        return json.load(config_file).get(key, None)
       
