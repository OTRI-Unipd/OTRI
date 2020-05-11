from ..filter import Filter, Stream, Collection
from typing import Mapping

class SummerFilter(Filter):
    '''
    Sums a constant to every atom.
    Input:
        Single stream 
    Output:
        Single stream
    '''

    def __init__(self, input_stream: Stream, keys_constants : Mapping):
        '''
        Parameters:
            input_streams : Stream
                Input stream.
            keys_constants : Mapping[str : float]
                Collection of keys whom values will be summed for the given constant.
        '''
        super().__init__(input_streams=[input_stream], input_streams_count=1, output_streams_count=1)
        self.__input_stream_iter = input_stream.__iter__()
        self.__output_stream = self.get_output_stream(0)
        self.__keys_consts = keys_constants

    def execute(self):
        '''
        Pops elements from the input streams sequentially (all of stream 0 then all of stream 1 and so on) and places them into the single output stream.
        '''
        if(self.__output_stream.is_closed()):
            return
        
        if(self.__input_stream_iter.has_next()):
            atom = next(self.__input_stream_iter)
            for key in self.__keys_consts.keys():
                atom[key] += self.__keys_consts[key]
            self.__output_stream.append(atom)
            
        elif(self.get_input_stream(0).is_closed()):
            self.__output_stream.close()
