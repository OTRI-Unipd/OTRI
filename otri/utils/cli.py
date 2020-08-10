__author__ = "Luca Crema <lc.crema@hotmail.com>"
__version__ = "1.0"

from typing import Sequence, Collection, List
import sys
import getopt


class CLIOpt:
    '''
    Defines a generic CLI option (eg. -p --parameter) that could take a value or could be just a flag.\n

    This is an abstract class, for implementations see:
    - CLIFlagOpt
    - CLIValueOpt
    '''

    def __init__(self, short_name: str = None, long_name: str = None, short_desc: str = None, long_desc: str = None):
        '''
        Parameters:\n
            short_name : str
                Single char identifier, eg. "a" will be used as "-a". Optional if long_name is defined.\n
            long_name : str
                Multiple chars identifier, eg. "apollo" will be used as "--apollo". Optional if short_name is defined.\n 
            short_desc : str
                Short description for the usage line, eg. "Custom file name" will be used in "-a <Custom file name>". Optional if long_desc is defined.\n
            long_desc : str
                Long description for the help page, eg. "The name for the analysier output file". Optional if short_desc is defined.\n
        '''
        self.short_name = short_name
        self.long_name = long_name
        self.short_desc = short_desc
        self.long_desc = long_desc

        if (short_name == None and long_name == None):
            print("CLI opt must define at least one of short name or long name")
        if (short_desc == None and long_desc == None):
            print("CLI opt must define at leasd one of short description or long description")

    def _takes_values(self) -> bool:
        '''
        Defines whether this option takes values or it's a flag.
        '''
        raise NotImplementedError

    def _name_list(self) -> List[str]:
        name_list = list()
        if self.short_name != None:
            name_list.append("-" + self.short_name)
        if self.long_name != None:
            name_list.append("--" + self.long_name)
        return name_list

    def _name_list_str(self) ->str:
        name_list = ""
        if self.short_name != None:
            name_list += "-" + self.short_name + " "
        if self.long_name != None:
            name_list += "--" + self.long_name + " "
        return name_list[:-1]

class CLIFlagOpt(CLIOpt):
    '''
    Defines a CLI option that does NOT take values, it's just a flag.
    '''

    def __init__(self, short_name: str = None, long_name: str = None, short_desc: str = None, long_desc: str = None):
        '''
        Parameters:\n
            short_name : str
                Single char identifier, eg. "a" will be used as "-a". Optional if long_name is defined.\n
            long_name : str
                Multiple chars identifier, eg. "apollo" will be used as "--apollo". Optional if short_name is defined.\n 
            short_desc : str
                Short description for the usage line, eg. "Custom file name" will be used in "-a <Custom file name>". Optional if long_desc is defined.\n
            long_desc : str
                Long description for the help page, eg. "The name for the analysier output file". Optional if short_desc is defined.\n
        '''
        super().__init__(short_desc=short_desc, long_desc=long_desc, short_name=short_name, long_name=long_name)

    def _takes_values(self) -> bool:
        '''
        It's a flag, it doesn't take any value.
        '''
        return False

class CLIValueOpt(CLIOpt):
    '''
    Defines a CLI option that requires the specification of a value which must be in a list of possible values.
    '''

    def __init__(self, short_name: str = None, long_name: str = None, short_desc: str = None, long_desc: str = None, required: bool = True, values: Collection[str] = None, default : str = None):
        '''
        Parameters:\n
            short_name : str
                Single char identifier, eg. "a" will be used as "-a". Optional if long_name is defined.\n
            long_name : str
                Multiple chars identifier, eg. "apollo" will be used as "--apollo". Optional if short_name is defined.\n 
            short_desc : str
                Short description for the usage line, eg. "Custom file name" will be used in "-a <Custom file name>". Optional if long_desc is defined.\n
            long_desc : str
                Long description for the help page, eg. "The name for the analysier output file". Optional if short_desc is defined.\n
            required : bool
                True if the option is needed, false if it's optional. By default it's required.\n
            values : Collection[str]
                Ordered collection of possible values that can be passed. If it's None any value will be accepted.\n
            default : str
                Default value for the option if it's not passed. Can be defined only if the option is not required.\n 
        '''
        super().__init__(short_desc=short_desc, long_desc=long_desc, short_name=short_name, long_name=long_name)
        self.values = values
        self.default = default
        self.required = required

        if required and default != None:
            print("CLI Value Opt cannot define a default value if the option is required")
    
    def _takes_values(self)->bool:
        '''
        It does take and requires values.
        '''
        return True


class CLI:
    '''
    Superclass that groups those tasks that an average CLI script must perform like:\n
    - Listing options and their possible values\n
    - Checking passed required option and if their values is acceptable\n
    - Define the '-h' or '--help' format or the usage instructions\n
    '''

    def __init__(self, name: str, description: str, options : Sequence[CLIOpt]):
        '''
        Parameters:\n
            name : str
                Name of the CLI script without the file extension (the '.py').\n
            description : str
                Description for the extended --help page.\n
            options : Sequence[CLIOpt]
                Sequence of options handled by the script.\n
        '''
        self.name = name
        self.description = description
        self.opt_list = options
        self.values = dict()

    def parse(self):
        '''
        Parses the options in the command line. If for any reason the parsing doesn't go well the program stops returning code 1.\n

        Returns:\n
            A dictionary where the key can be either "-<short_name>" or "--<long_name>" and the value is the passed argument.
        '''
        # Find options and arguments
        try:
            opts = getopt.getopt(sys.argv[1:], self.__build_opt_string(), self.__build_opt_list())[0]
        except getopt.GetoptError as e:
            print(e)
            print(self.__build_usage_line())
            quit(1)

        # Initialise default values
        for cliopt in self.opt_list:
            for name in cliopt._name_list():
                if cliopt._takes_values():
                    self.values[name] = cliopt.default
                else:
                    self.values[name] = False
        
        # Analyse every option passed
        for opt, arg in opts:
            if opt in ("-h", "--help"):
                print(self.__build_help_page())
                quit(1)
            for cliopt in self.opt_list:
                if opt in cliopt._name_list():
                    if cliopt._takes_values():
                        # Check if the value is acceptable
                        if cliopt.values != None:
                            if not arg in cliopt.values:
                                print("Unsupported value for option {}: {}".format(opt, arg))
                                print(self.__build_usage_line())
                                quit(1)
                            # Save its value for later retrieval
                        for name in cliopt._name_list():
                            self.values[name] = arg
                    else:
                        # It's just a flag, set its value to true
                        for name in cliopt._name_list():
                            self.values[name] = True
                    break
            else:
                # Should never get here, if an option is not supported an error will be raised by getopt(...)
                print("Unsupported option {}".format(opt))
                print(self.__build_usage_line())
                quit(1)
        
        for cliopt in self.opt_list:
            if cliopt._takes_values() and cliopt.required:
                cli_name = cliopt._name_list()[0] # Either short or long name
                if self.values[cli_name] == None:
                    print("Missing required argument: {}".format(cli_name))
                    print(self.__build_usage_line())
                    quit(1)

        return self.values

    def add_option(self, param: CLIOpt):
        '''
        Adds an option to the list of options for the script.
        '''
        self.opt_list.append(param)

    def add_options(self, params: Sequence[CLIOpt]):
        '''
        Adds an ordered sequence of options to the list of options for the script.
        '''
        self.opt_list.extend(params)
    
    def __build_opt_string(self) -> str:
        '''
        Builds the short opt string used in getopt() method.\n
        "h" is included.
        '''
        shortopt = "h"
        for opt in self.opt_list:
            if opt.short_name != None:
                shortopt += opt.short_name
                if opt._takes_values():
                    shortopt += ":"
        return shortopt

    def __build_opt_list(self) -> List[str]:
        '''
        Builds the long opt list of strings used in getopt() method.\n
        "--help" is included.
        '''
        longopt = ["help"]
        opt_string = ""
        for opt in self.opt_list:
            if opt.long_name != None:
                opt_string = opt.long_name
                if opt._takes_values():
                    opt_string += "="
                longopt.append(opt_string)
        return longopt

    def __build_usage_line(self) -> str:
        '''
        Builds the short help line for usage.\n
        eg: "Usage: script.py [-a <some name ['Roberto', 'Ignazio', 'Gianfranco']>]".
        '''
        line = "Usage: {}.py ".format(self.name)
        opt_str = desc_str = ""
        for opt in self.opt_list:
            # Name: "-a" or "--apollo"
            if opt.short_name != None:
                opt_str = "-" + opt.short_name
            else: opt_str = "--" + opt.long_name
            # Description "<desc>" or "<desc [v1,v2,v3]>" or "<[v1,v2,v3]>"
            if opt._takes_values():
                if opt.short_desc != None:
                    if opt.values != None:
                        desc_str = "<{} {}>".format(opt.short_desc, opt.values)
                    else:
                        desc_str = "<{}>".format(opt.short_desc)
                elif opt.values != None:
                    desc_str = "<{}>".format(opt.values)
            else:
                desc_str = "<{}>".format(opt.short_desc)
            if opt._takes_values() and not opt.required:
                opt_str = "[{} {}]".format(opt_str, desc_str)
            else:
                opt_str = "{} {}".format(opt_str, desc_str)
            line += "{} ".format(opt_str)
        return line
        
    def __build_help_page(self)->str:
        '''
        Builds the help page as string.\n
        The help page contains the usage line and the description of every parameter
        '''
        usage_line = self.__build_usage_line()
        help_page = "{} \n\n{}\n\nParameters:\n\n".format(usage_line, self.description)
        # Alignment for description
        max_names_len = 1
        for opt in self.opt_list:
            if len(opt._name_list_str()) > max_names_len:
                max_names_len = len(opt._name_list_str())
        for opt in self.opt_list:
            help_page += "    {}".format(opt._name_list_str())
            for i in range(0,max_names_len - len(opt._name_list_str())):
                help_page += " "
            help_page += "    {}\n".format(opt.long_desc)
            if opt._takes_values() and opt.values != None:
                help_page += "    Values: {}\n".format(opt.values)
            help_page += "\n"
        return help_page