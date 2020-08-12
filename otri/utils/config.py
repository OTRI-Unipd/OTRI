"""
Its purpose is to read configuration values from container secret files or a json config.

Settings:
If json config filename is not the default 'config.json' change the json_config_filename variable.
If sectets foldername is not the default `secrets` change the secrets_foldername variable.
"""

__version__ = '1.0'
__all__ = [
    'get_value'
]

__author__ = 'Luca Crema <lc.crema@hotmail.com>'

import json
from pathlib import Path

__cache = dict()
json_config_filename = "config.json"
secrets_foldername = "secrets"

def get_value(key: str, default = None) -> str:
    '''
    Reads config json file or secrets and looks for the given key. Values are cached.

    Parameters:
        key : str
            Requested configuration key.\n
        default : str
            Default value if the file is missing or the configuration is missing
    Returns:
        str containing the value if the key was found in the config file or secrets folder, None otherwise.
    '''
    # Check if value is cached
    if(key in __cache):
        return __cache[key]

    # Load keys from all kind of storage
    if(has_config()):
        config_file = Path(json_config_filename).open("r")
        with config_file as json_file:
            json_dict = json.load(json_file)
            __cache.update(json_dict)

    if(has_secret()):
        files = [x for x in Path(secrets_foldername).iterdir() if x.is_file()]
        for f in files:
            with f.open("r") as contents:
                __cache[f.name] = contents.readline()
    # If couln't find the key set the default value
    if not key in __cache:
        __cache[key] = default
    return __cache[key]
       
def has_config() -> bool:
    if Path(json_config_filename).is_file():
        return True
    return False

def has_secret() -> bool:
    if Path(secrets_foldername).is_dir():
        return True
    return False