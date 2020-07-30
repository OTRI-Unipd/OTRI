"""
Module to log errors and warnings on a LOG.txt file.
Everything from verbose up (levels 1-4) gets printed in the console too.
"""
__version__ = "1.0"
__all__ = ["v", "d", "i", "w", "e"]
__author__ = "Luca Crema <lc.crema@hotmail.com>"

from typing import Tuple
from datetime import datetime
from termcolor import colored
from pathlib import Path

import os
import sys


def __caller_info() -> Tuple[str, int]:
    '''
    Gets some information about the module caller.
    Returns:
        The filename of the caller.
        The line number of the call.
    '''
    # Get caller class and method
    frame = sys._getframe(3)
    # Get rid of absolute path
    filename = os.path.splitext(os.path.basename(frame.f_code.co_filename))[0]
    return filename, frame.f_lineno


def __make_file():
    '''
    Ensures LOG_DIR exists.
    Returns:
        The Path to the default log file.
    '''
    LOG_DIR.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y_%m_%d_%H_%M_%S_%f")[:-3]
    return Path(LOG_DIR, "logger_{}.txt".format(timestamp))


def __time():
    '''
    Returns:
        A formatted timestamp for logging.
    '''
    time = datetime.now()
    return time.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


# Log files subdirectory.
LOG_DIR = Path("log/")
# File will always be called "logger_timestamp" by default.
LOG_FILE = None

NAMES = (
    "VERBOSE",
    "DEBUG",
    "INFO",
    "WARNING",
    "ERROR"
)

COLORS = (
    "grey",
    "cyan",
    "green",
    "yellow",
    "red"
)

min_console_priority = -1



def v(msg: str, log_file: Path = None):
    '''
    Logs a VERBOSE message.
    Parameters:
        msg : str
            Content of the message to log.
        log_file : Path
            The log file where the output should be written.
    '''
    _log(0, msg, log_file)


def d(msg: str, log_file: Path = None):
    '''
    Logs a DEBUG message.
    Parameters:
        msg : str
            Content of the message to log.

        log_file : Path
            The log file where the output should be written.
    '''
    _log(1, msg, log_file)


def i(msg: str, log_file: Path = None):
    '''
    Logs a INFO message.
    Parameters:
        msg : str
            Content of the message to log.
        log_file : Path
            The log file where the output should be written.
    '''
    _log(2, msg, log_file)


def w(msg: str, log_file: Path = None):
    '''
    Logs a WARNING message.
    Parameters:
        msg : str
            Content of the message to log.
        log_file : Path
            The log file where the output should be written.
    '''
    _log(3, msg, log_file)


def e(msg: str, log_file: Path = None):
    '''
    Logs an ERROR message.
    Parameters:
        msg : str
            Content of the message to log.
        log_file : Path
            The log file where the output should be written.
    '''
    _log(4, msg, log_file)


def _log(priority: int, msg: str, log_file: Path):
    '''
    Logs the message into the log file keeping track of the datetime and priority level.

    Parameters:
        priority : int
            Level of priority, must be within 0 and 4 where 0 is a normal message and 4 is a critical error.
        msg : str
            Content of the message to log.
        log_file : Path
            The log file to which the output should be sent.
    '''
    if not log_file:
        # Declaring as global necessary.
        global LOG_FILE

        if not LOG_FILE:
            LOG_FILE = __make_file()
        
        log_file = LOG_FILE

    # Get current datetime
    time = __time()
    # Translate priority into a word
    priority_name = NAMES[priority]
    # Get caller data
    caller, lineno = __caller_info()
    # Build the line
    console_line = colored(
        "{} {}:{} - {}".format(priority_name, caller, lineno, msg),
        COLORS[priority]
    )
    file_line = "{} {} {}:{} - {}".format(priority_name, time, caller, lineno, msg)
    # Write on file
    with log_file.open("a") as f:
        f.write(file_line + "\n")
    # Print on console if needed
    if(priority >= 1):
        print(console_line)
