from typing import Collection, Callable
from .filter import Filter

class FilterLayer:
    '''
    Defines a list of filters that could be executed in parallel.
    '''

    def __init__(self, filters : Collection[Filter], policy : Callable):
        self.filters = filters
        self.policy = policy
    
    def set_policy(self, policy : Callable):
        self.policy = policy

    def call_policy(self)->int:
        return self.policy(self)

    def has_outputted(self):
        for f in self.filters:
            if f._has_outputted:
                return True
        return False

    def has_finished(self):
        '''
        Checks if all of the input streams of the filters are closed.
        '''
        for f in self.filters:
            for s in f._get_inputs():
                if not s.is_closed():
                    return False
        return True
