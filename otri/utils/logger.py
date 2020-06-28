"""
Module to log errors and warnings on a LOG.txt file.
"""
__version__ = "1.0"
__all__ = ["v","d","i","w","e","log"]
__author__ = "Luca Crema <lc.crema@hotmail.com>"

from pathlib import Path
from datetime import datetime

import sys

FILENAME = "LOG.txt"
NAMES = [
    "VERBOSE",
    "DEBUG",
    "INFO",
    "WARNING",
    "ERROR"
]

min_console_priority = -1

def v(msg: str):
    """
    Logs a VERBOSE message.
    Parameters:
        msg : str
            Content of the message to log.
    """
    log(0, msg)


def d(msg: str):
    """
    Logs a DEBUG message.
    Parameters:
        msg : str
            Content of the message to log.
    """
    log(1, msg)


def i(msg: str):
    """
    Logs a INFO message.
    Parameters:
        msg : str
            Content of the message to log.
    """
    log(2, msg)


def w(msg: str):
    """
    Logs a WARNING message.
    Parameters:
        msg : str
            Content of the message to log.
    """
    log(3, msg)


def e(msg: str):
    """
    Logs an ERROR message.
    Parameters:
        msg : str
            Content of the message to log.
    """
    log(4, msg)


def log(priority: int, msg: str):
    """
    Logs the message into the log file keeping track of the datetime and priority level.

    Parameters:
        priority : int
            Level of priority, must be within 0 and 4 where 0 is a normal message and 4 is a critical error.
        msg : str
            Content of the message to log.
    """
    # Get current datetime
    time = __time()
    # Translate priority into a word
    priority_name = NAMES[priority]
    # Get caller class and method
    caller_class = None
    frame = sys._getframe()
    while(caller_class == None):
        frame = frame.f_back
        try:
            caller_class = frame.f_locals["self"].__class__.__name__
            caller_method = frame.f_code.co_name
        except KeyError:
            # Means that it's not been called from a class, go back again until we find a class
            pass
    # Build the line
    console_line = "{} {}.{}: {}".format(priority_name, caller_class, caller_method, msg)
    file_line = "{} {} {}.{}: {}".format(priority_name, time, caller_class, caller_method, msg)
    # Write on file
    f = open(FILENAME, "a")
    f.write(file_line + "\n")
    # Print on console if needed
    if(priority >= 1):
        print(console_line)


def __time():
    time = datetime.now()
    return time.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
