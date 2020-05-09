from typing import Collection
from .stream import Stream


class Filter:
    '''
    Abstract class that defines an atom manipulation filter.

    Attributes:
        input_streams : Collection[Stream]
            Streams where to dequeue atoms from.
        output_streams : Collection[Stream]
            Streams where manipulated atoms are placed.
    '''

    def __init__(self, input_streams: Collection[Stream], input_streams_count: int = 0, output_streams_count: int = 0):
        '''
        Parameters:
            input_streams : Collection[Stream]
                Collection of streams where to source atoms.
            input_streams_count : int
                The number of input streams that the filter handles.
            output_streams_count : int
                The number of output streams that the filter handles.
        Raises:
            ValueError
                if the given input_streams collection has a different number of elements than expected.
        '''
        if(len(input_streams) != input_streams_count):
            raise ValueError("This filter takes {} streams, {} given".format(
                input_streams_count, len(input_streams)))
        self.input_streams_count = input_streams_count
        self.output_streams_count = output_streams_count
        self.output_streams = [Stream()] * output_streams_count
        self.input_streams = input_streams

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

    def get_input_stream(self, index) -> Stream:
        '''
        Retrieve a sepecific input stream.
        '''
        return self.input_streams[index]

    def get_output_streams(self) -> Collection[Stream]:
        '''
        Retrieve the defined collection of output streams.
        '''
        return self.output_streams

    def get_output_stream(self, index: int) -> Stream:
        '''
        Retrieves a specific output stream.
        '''
        if(hasattr(self, 'output_streams')):
            return self.output_streams[index]
        else:
            raise NotImplementedError(
                "The filter has not defined its output list with set_output_streams")

    def is_finished(self)-> bool:
        '''
        Checks whether all of the output streams are flagged as finished, meaning that no more atoms will be added.
        '''
        for output in self.get_output_streams():
            if not output.is_finished():
                return False
        return True