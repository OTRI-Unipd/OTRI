from ..filter import Filter, Stream, Collection
from typing import Mapping, Callable


class MathFilter(Filter):
    '''
    Performs a give operation on keys of an item.
    Input:
        Single stream 
    Output:
        Single stream
    '''

    def __init__(self, input_stream: Stream, keys_operations: Mapping[str, Callable]):
        '''
        Parameters:
            input_streams : Stream
                Input stream.
            keys_operations : Mapping[str : Callable]
                Collection of keys whom values will be summed for the given constant.
        '''
        super().__init__(input_streams=[input_stream],
                         input_streams_count=1, output_streams_count=1)
        self.__input_stream_iter = input_stream.__iter__()
        self.__output_stream = self.get_output_stream(0)
        self.__keys_operations = keys_operations

    def execute(self):
        '''
        Performs given operations on keys of the item.
        '''
        if(self.__output_stream.is_closed()):
            return

        if(self.__input_stream_iter.has_next()):
            atom = next(self.__input_stream_iter)
            for key in self.__keys_operations.keys():
                atom[key] = self.__keys_operations[key](atom[key])
            self.__output_stream.append(atom)

        elif(self.get_input_stream(0).is_closed()):
            self.__output_stream.close()
