from ..filter import Filter, Stream, Collection


class MultiplierFilter(Filter):
    '''
    Multiplies i atom's given keys with i+c atom's keys
    Input:
        Single stream ordered by datetime
    Output:
        A single stream containing n - c atoms
    '''

    def __init__(self, input_stream: Stream, keys_to_change: Collection[str], distance : int):
        '''
        Parameters:
            input_streams : Stream
                Input stream.
            keys_to_change : Collection[str]
                Collection of keys whom values will be multiplied.
            distance : int
                Distance in number of atoms to calculate a[i] * a[i+c]
        '''
        super().__init__(input_streams=[input_stream], input_streams_count=1, output_streams_count=1)
        self.__input_stream_iter = input_stream.__iter__()
        self.__output_stream = self.get_output_stream(0)
        self.__keys = keys_to_change
        self.__distance = distance
        self.__atoms_buffer = list()
        self.__counter = 0

    def execute(self):
        '''
        Pops elements from the input streams sequentially (all of stream 0 then all of stream 1 and so on) and places them into the single output stream.
        '''
        if(self.__output_stream.is_closed()):
            return
        
        if(self.__input_stream_iter.has_next()):
            if(len(self.__atoms_buffer) < self.__counter + 1):
                self.__atoms_buffer.append(next(self.__input_stream_iter))
            else:
                atom_1 = self.__atoms_buffer[self.__counter]
                atom_2 = (next(self.__input_stream_iter))
                mul_atom = {}
                for key in self.__keys:
                    mul_atom[key] = atom_1[key] * atom_2[key]
                self.__atoms_buffer[self.__counter] = atom_2
                self.__output_stream.append(mul_atom)
            self.__counter = (self.__counter + 1) % self.__distance
        elif(self.get_input_stream(0).is_closed()):
            self.__output_stream.close()
