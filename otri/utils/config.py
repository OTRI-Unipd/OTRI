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

__cache = dict()

def get_value(key: str, default = None, filename: str = "config") -> str:
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
    # Read the file only if it's never been opened
    if not filename in __cache:
        json_file = None
        try:
            json_file = Path("{}.json".format(filename)).open("r")
        except FileNotFoundError:
            return default
        
        with json_file as config_file:
            __cache[filename] = json.load(config_file)

    return __cache[filename].get(key, default)
       
