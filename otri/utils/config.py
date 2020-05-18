import json
from pathlib import Path

def get_value(key: str, filename: str = "config"):
    '''
    Reads config file and looks for the given key.

    Parameters:
        key : str
            Requested configuration key
    Returns:
        str containing the value if the key was found in the config file, None otherwise
    '''
    try:
        with Path("{}.json".format(filename)).open("r") as config_file:
            return json.load(config_file).get(key, None)
    except FileNotFoundError:
        return None
