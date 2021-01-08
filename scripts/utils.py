"""
Generically useful functions for running other Python scripts
"""

import os
import sys
import docopt
import re
import contextlib


def suppress_stdout(func):
    """
    Decorator to suppress stdout from a given function. Taken from 
    https://stackoverflow.com/a/28321717. Note that contextlib.redirect_stdout() 
    was introduced in Python 3.4 so this is a py3 solution only

    Usage:
    @suppress_stdout
    def func():
        print('something')

    func() # nothing prints
    """
    def wrapper(*args, **kwargs):
        with open(os.devnull, 'w') as devnull:
            with contextlib.redirect_stdout(devnull):
                func(*args, **kwargs)
                
    return wrapper


def get_cl_args(doc):
    """
    Get command line arguments as a dictionary
    :return: dictionary of arguments
    """
    # Any args that don't have a default value and weren't specified will be None
    cl_args = {k: v for k, v in docopt.docopt(doc).items() if v is not None}

    # get rid of extra characters from doc string and 'help' entry
    args = {re.sub('[<>-]*', '', k): v for k, v in cl_args.items() if k != '--help' and k != '-h'}

    # convert numeric values
    for k, v in args.items():
        if type(v) == bool or v == None:
            continue
        elif re.fullmatch('\d*', v):
            args[k] = int(v)
        elif re.fullmatch('\d*\.\d*', v):
            args[k] = float(v)

    return args