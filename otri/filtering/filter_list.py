from typing import Callable, Iterable
from .filter_layer import FilterLayer
from .filter import Filter, Collection, Stream, StreamIter
from .filters.successive_source_filter import SuccessiveSourceFilter


class FilterList:
    '''
    Ordered collection of filter layers.
    '''

    def __init__(self, layers: Collection[FilterLayer]):
        '''
        Parameters:
            layers : Collection[FilterLayer]
                Collection of layers to execute in order.
        '''
        self.layers = layers

    def execute(self, source_streams: Collection[Stream], on_atom_output: Callable):
        '''
        Starts working on the origin stream with the given filter layers

        Parameter:
            source_streams : Collection[Streams]
                Atoms collections. Required iterator that has `has_next()` method.
        '''
        # Add the source filter
        self.__add_source_filter(source_streams)
        # Grab the output iterator
        last_output_iterator = self.layers[len(
            self.layers) - 1][0].get_output_stream(0).__iter__()

        while(not self.__is_all_finished()):
            for filter_layer in self.layers:
                for filter in filter_layer:
                    filter.execute()
            # If any filter has outputted any atom, call the on_atom_output method
            while(last_output_iterator.has_next()):
                on_atom_output(next(last_output_iterator))

    def __add_source_filter(self, source_streams: Collection[Stream]):
        '''
        Adds a source filter as the first layer of the filter layer list.
        The filter is needed because it's the only one that can set itself is_finished to True if the input stream is finished, and is needed
        to apply the recursive definition of is_finished.
        '''
        origin_filter = SuccessiveSourceFilter(source_streams)
        self.layers = [FilterLayer([origin_filter])] + self.layers

    def __is_all_finished(self) -> bool:
        '''
        Checks if every filter layer's filters are flagged as finished
        '''
        for filter_layer in self.layers:
            if not filter_layer.is_finished():
                return False
        return True
