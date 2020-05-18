import json
from pathlib import Path

class Config:
    '''
    Used to load configurations from a config.json file that contains private information and shoul never be in the repository
    '''

    @staticmethod
    def get_config(config_name : str):
        '''
        Reads config file and looks for the given key.

        Parameters:
            config_name : str
                Requested configuration key
        Returns:
            str containing the value if the key was found in the config file, None otherwise
        '''
        try:
            with Path("config.json").open("r") as config_file:
                return json.load(config_file).get(config_name, None)
        except FileNotFoundError:
            return None