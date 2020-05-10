from typing import Collection, Callable
from .filter_layer import FilterLayer
from .stream import Stream


class FilterList:
    '''
    Ordered collection of filter layers.
    '''

    def __init__(self):
        self.layers = []

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

    def get_output_streams(self):
        '''
        Retrieves all of the output streams.
        '''
        output_streams = []
        for f in self.layers[len(self.layers) -1]:
            output_streams.extend(f.get_output_streams)

    def __is_all_finished(self) -> bool:
        '''
        Checks if the last filter layer's filters are flagged as finished
        '''
        if not self.layers[len(self.layers) -1].is_finished():
            return False
        return True
