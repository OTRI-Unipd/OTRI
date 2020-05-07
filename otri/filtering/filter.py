from typing import Collection, Iterable
from .stream import Stream

class Filter:
    '''
    Abstract class that defines an atom manipulation filter.

    Attributes:
        input_streams : Collection[Iterable]
            Streams where to dequeue atoms from.
        is_finished : bool
            Declares whether this filter has finished elaborating atoms, this depends on upper layers filters and input streams.
    '''

    def __init__(self, input_streams : Collection[Iterable]):
        '''
        Parameters:
            input_streams : Collection[Stream]
                Collection of streams where to source atoms.
        '''
        if(len(input_streams) != self.__get_input_stream_numbers()):
            raise ValueError("This filter only takes {} streams, {} given",self.__get_input_stream_numbers, len(input_streams))
        self.input_streams = input_streams
        self.is_finished = False


    def execute(self):
        '''
        This method gets called when the filter could have new atoms to manipulate.
        '''
        raise NotImplementedError("Filter is an abstract class, please implement this method in a subclass")

    def __get_input_stream_numbers(self)->int:
        '''
        TODO: Documentation
        '''
        raise NotImplementedError("The filter did not define how many input stream it uses, define it using __get_input_stream_numbers()")

    def get_input_streams(self)->Iterable:
        '''
        TODO: Documentation
        '''
        return self.input_streams

    def get_output_streams(self)->Iterable[Stream]:
        '''
        TODO: Documentation
        '''
        raise NotImplementedError("Filter is an abstract class, please implement this method in a subclass")

