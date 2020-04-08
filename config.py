import json

class Config:
    '''
    Used to load configurations from a config.json file that contains private information and shoul never be in the repository
    '''

    def __init__(self):
        '''
        Opens config.json file, ready to load configuration variables
        '''
        with open('config.json') as config_file:
            self.config_data = json.load(config_file)

    def get_config(self, config_name : str):
        '''
        Reads config file and looks for the given key.

        Parameters:
            config_name : str
                Requested configuration key
        Returns:
            str containing the value if the key was found in the config file, None otherwise
        '''
        return self.config_data[config_name]