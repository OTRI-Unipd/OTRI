import os
import otri
from setuptools import find_packages
from pkgutil import iter_modules
from pathlib import Path
from typing import Set


def get_otri_modules() -> Set[str]:
    '''
    Returns:
        A set with the names of all packages and modules in 'otri'.
    '''
    path = Path(os.path.dirname(otri.__file__))
    modules = set()
    for pkg in find_packages(str(path)):
        modules.add(pkg)
        pkgpath = Path(path, pkg.replace('.', '/'))
        for info in iter_modules([pkgpath]):
            if not info.ispkg:
                modules.add(pkg + '.' + info.name)
    return modules
