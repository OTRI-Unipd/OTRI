import os
import otri
from setuptools import find_packages
from pkgutil import iter_modules
from pathlib import Path


def get_otri_modules():

    path = Path(os.path.dirname(otri.__file__))
    print(path)
    modules = set()
    for pkg in find_packages(str(path)):
        modules.add(pkg)
        pkgpath = Path(path, pkg.replace('.', '/'))
        for info in iter_modules([pkgpath]):
            if not info.ispkg:
                modules.add(pkg + '.' + info.name)
    print(modules)
    return modules
