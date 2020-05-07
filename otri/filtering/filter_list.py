from typing import Iterable
from .filter_layer import FilterLayer
from .filter import Filter, Collection, Iterable

class FilterList:
    '''
    Ordered collection of filter layers.
    '''

    def __init__(self, layers : Collection[FilterLayer]):
        '''

        Parameters:
            layers : Collection[FilterLayer]
                Collection of layers to execute in order.
        '''
        self.layers = layers

    def execute(self, origin_stream_iterator):
        '''
        Starts working on the origin stream with the given filter layers

        Parameter:
            origin_stream_iterator : Iterator
                Atoms collection iterator with methods `next()` and `hasNext()`
        '''
        origin_filter = 
        while(not self.__is_all_finished()):
            for filter_layer in self.layers:
                for filter in filter_layer:
                    filter.execute()

    def __is_all_finished(self):
        for filter_layer in self.layers:
            for filter in filter_layer:
                if not filter.is_finished
                    return False
        return True