'''
Runs tests, profiles them and prints the result to a "test.prof" file.
It then opens such file with `snakeviz`.
'''

__author__ = "Riccardo De Zen <riccardodezen98@gmail.com>"
__version__ = "2.0"

# import os
import cProfile
import pytest

with cProfile.Profile() as pf:
    pytest.main(["-s", "--cov=otri", "test/"])
    pf.dump_stats("test.prof")

# os.system("python -m snakeviz test.prof")
