from typing import Collection, Callable
from .filter_layer import FilterLayer
from .stream import Stream
from .filters.successive_source_filter import SuccessiveSourceFilter


class FilterList:
    '''
    Ordered collection of filter layers.
    '''

    def __init__(self, source_streams: Collection[Stream]):
        '''
        Parameters:
            layers : Collection[FilterLayer]
                Collection of layers to execute in order.
        '''
        self.layers = []
        self.__add_source_filter(source_streams)
    
    def add_layer(self, layer : FilterLayer):
        '''
        Appends a layer at the end of the list

        Parameters:
            layer : FilterLayer
                Non-empty layer of filters.
        '''
        self.layers.append(layer)

    def execute(self, on_atom_output: Callable, on_execute_finished : Callable = None):
        '''
        Starts working on the origin stream with the given filter layers

        Parameter:
            source_streams : Collection[Streams]
                Atoms collections. Required iterator that has `has_next()` method.
        '''
        # Grab the output streams
        last_output_streams = self.layers[len(
            self.layers) - 1][0].get_output_streams()
        last_output_iterators = [x.__iter__() for x in last_output_streams]

        while(not self.__is_all_finished()):
            for filter_layer in self.layers:
                for fil in filter_layer:
                    fil.execute()
            # If any filter has outputted any atom, call the on_atom_output method
            for iterator in last_output_iterators:
                while(iterator.has_next()):
                    on_atom_output(next(iterator))
        if (on_execute_finished != None):
            on_execute_finished()

    def get_stream_output(self):
        '''
        Retrieves the stream reading output
        '''
        return self.layers[0][0].get_output_stream(0)

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
