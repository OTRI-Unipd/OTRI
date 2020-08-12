'''
Contains profiling utilities.
'''

__autor__ = "Luca Crema <lc.crema@hotmail.com>"
__version__ = "1.0"

import cProfile
import threading


class ProfiledThread(threading.Thread):

    def run(self):
        profiler = cProfile.Profile()
        try:
            return profiler.runcall(self.crun)
        finally:
            profiler.dump_stats('{}-{}.profile'.format(self.__name__, self.ident))

    def crun(self):
        pass
