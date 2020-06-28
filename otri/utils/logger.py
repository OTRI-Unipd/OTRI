"""
Module to log errors and warnings on a LOG.txt file (for extra style and colors).
Everything from verbose up (levels 1-4) gets printed in the console too.
"""
__version__ = "1.0"
__all__ = ["v", "d", "i", "w", "e", "log"]
__author__ = "Luca Crema <lc.crema@hotmail.com>"

from pathlib import Path
from datetime import datetime
from termcolor import colored

import os
import sys

FILENAME = "LOG.txt"
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


def v(msg: str):
    """
    Logs a VERBOSE message.
    Parameters:
        msg : str
            Content of the message to log.
    """
    _log(0, msg)


def d(msg: str):
    """
    Logs a DEBUG message.
    Parameters:
        msg : str
            Content of the message to log.
    """
    _log(1, msg)


def i(msg: str):
    """
    Logs a INFO message.
    Parameters:
        msg : str
            Content of the message to log.
    """
    _log(2, msg)


def w(msg: str):
    """
    Logs a WARNING message.
    Parameters:
        msg : str
            Content of the message to log.
    """
    _log(3, msg)


def e(msg: str):
    """
    Logs an ERROR message.
    Parameters:
        msg : str
            Content of the message to log.
    """
    _log(4, msg)


def _log(priority: int, msg: str):
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
    frame = sys._getframe(2)
    # Get rid of absolute path
    filename = os.path.splitext(os.path.basename(frame.f_code.co_filename))[0]
    # Build the line
    console_line = colored("{} {}:{} - {}".format(priority_name,
                                                  filename, frame.f_lineno, msg), COLORS[priority])
    file_line = colored("{} {} {}:{} - {}".format(priority_name, time, filename, frame.f_lineno, msg),COLORS[priority])
    # Write on file
    f = open(FILENAME, "a")
    f.write(file_line + "\n")
    # Print on console if needed
    if(priority >= 1):
        print(console_line)


def __time():
    time = datetime.now()
    return time.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
