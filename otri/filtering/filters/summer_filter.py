from ..filter import Filter, Stream, Collection


class SummerFilter(Filter):
    '''
    Sums a constant to every atom.
    Input:
        Single stream 
    Output:
        Single stream
    '''

    def __init__(self, input_stream: Stream, keys_to_change: Collection[str], const : int):
        '''
        Parameters:
            input_streams : Stream
                Input stream.
            keys_to_change : Collection[str]
                Collection of keys whom values will be multiplied.
            const : float
               Constant number to add (or subtract) to every keys_to_change of every atom
        '''
        super().__init__(input_streams=[input_stream], input_streams_count=1, output_streams_count=1)
        self.__input_stream_iter = input_stream.__iter__()
        self.__output_stream = self.get_output_stream(0)
        self.__keys = keys_to_change
        self.__const = const

    def execute(self):
        '''
        Pops elements from the input streams sequentially (all of stream 0 then all of stream 1 and so on) and places them into the single output stream.
        '''
        if(self.__output_stream.is_closed()):
            return
        
        if(self.__input_stream_iter.has_next()):
            atom = next(self.__input_stream_iter)
            for key in self.__keys:
                atom[key] = atom[key] + self.__const
            self.__output_stream.append(atom)
            
        elif(self.get_input_stream(0).is_closed()):
            self.__output_stream.close()
