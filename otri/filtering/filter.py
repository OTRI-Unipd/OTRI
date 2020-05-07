from typing import Collection, Iterator
from .stream import Stream, StreamIter


class Filter:
    '''
    Abstract class that defines an atom manipulation filter.

    Attributes:
        input_streams : Collection[Stream]
            Streams where to dequeue atoms from.
        is_finished : bool
            Declares whether this filter has finished elaborating atoms, this depends on upper layers filters and input streams.
    '''

    def __init__(self, input_streams: Collection[Stream], input_streams_count = 0, output_streams_count = 0):
        '''
        Parameters:
            input_streams : Collection[Stream]
                Collection of streams where to source atoms.
        Raises:
            ValueError
                if the given input_streams collection has a different number of elements than expected.
        '''
        if(len(input_streams) != input_streams_count):
            raise ValueError("This filter only takes {} streams, {} given".format(input_streams_count, len(input_streams)))
        self.output_streams = [Stream()] * output_streams_count
        self.input_streams = input_streams
        self.is_finished = False

    def execute(self):
        '''
        This method gets called when the filter could have new atoms to manipulate.
        It should:
        - Pop a single atom from any of the input streams
        - Elaborate it and optionally update the filter state
        - Push it into one of the output streams
        '''
        raise NotImplementedError(
            "Filter is an abstract class, please implement this method in a subclass")

    def get_input_streams(self) -> Collection[Stream]:
        '''
        Retrieve the input streams.
        '''
        return self.input_streams

    def get_output_streams(self) -> Collection[Stream]:
        '''
        Retrieve the defined output streams.
        '''
        if(hasattr(self, 'output_streams')):
            return self.output_streams
        else:
            raise NotImplementedError(
                "The filter has not defined its output list with set_output_streams")
