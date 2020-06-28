"""
Its purpose is to read values from top-level JSON file's keys to speed up configuration strings reading.
"""

__version__ = '1.0'
__all__ = [
    'get_value'
]

__author__ = 'Luca Crema <lc.crema@hotmail.com>'

import json
from pathlib import Path

__open_files = dict()

def get_value(key: str, default = None,filename: str = "config") -> str:
    '''
    Reads config file and looks for the given key.

    Parameters:
        key : str
            Requested configuration key.
        default : str
            Default value if the file is missing or the configuration is missing
        filename : str
            Name of a json file. Must not contain the file extension.
    Returns:
        str containing the value if the key was found in the given config file, None otherwise.
    '''
    if(__open_files.get(filename, None) == None):
        try:
            __open_files[filename] = Path("{}.json".format(filename)).open("r")
        except FileNotFoundError:
            return default
    
    with __open_files[filename] as config_file:
        return json.load(config_file).get(key, default)
       
