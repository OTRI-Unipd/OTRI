__author__ = "Luca Crema <lc.crema@hotmail.com>"
__version__ = "1.0"

from typing import Sequence
import sys
import getopt
import .logger as log

class CLIParam:
    '''
    Defines a CLI parameter.
    '''
    def __init__(self, short_desc : str, long_desc : str = None, short_name : str = None, required : bool = True, is_flag  : bool = False, long_name : str = None, values : Sequence[str] = None):
        '''
        TODO
        Parameters:\n
        '''
        self.short_name = short_name
        self.long_name = long_name
        self.short_desc = short_desc
        self.long_desc = long_desc
        self.required = required
        self.is_flag = is_flag
        self.values = values

        if (short_name == None and long_name == None):
            log.e("CLI class must define at least one of short name and long name")
        if (is_flag and not values == None):
            log.e("CLI class cannot define values if it's a flag")


class CLI:
    '''
    Superclass that groups those tasks that an average CLI script must perform like:\n
    - Listing parameters and their possible values\n
    - Checking passed required parameters and if theri values is acceptable\n
    - Define the '-h' or '--help' format or the usage instructions\n
    '''

    def __init__(self, name : str, description : str):
        '''
        Parameters:\n
            name : str\n
                Name of the CLI script.\n
            description : str\n
                Description for the --help page.\n
        '''
        self.name = name
        self.description = description
        self.param_list = []

    def add_parameter(self, param : CLIParam):
        '''
        Adds a parameter to the list of parameters for the script
        '''
        self.param_list.append(param)
        # TODO