from ..filter import Filter, Stream, Collection


class SequentialMergeFilter(Filter):
    '''
    Merges elements from multiple streams into one single output.
    Input:
        Multiple streams.
    Output:
        A single stream containing data read sequentially (all of stream 1, then all of stream 2 and so on).
    '''

    def __init__(self, input_streams: Collection[Stream]):
        '''
        Parameters:
            input_streams : Collection[Stream]
                Collection of input streams.
        '''
        super().__init__(input_streams=input_streams,
                         input_streams_count=len(input_streams), output_streams_count=1)
        # caching iterators to avoid creating a new one every time execute() is called
        self.input_iterators = [x.__iter__() for x in input_streams]
        self.output_stream = self.get_output_stream(0)

    def execute(self):
        '''
        Pops elements from the input streams sequentially (all of stream 0 then all of stream 1 and so on) and places them into the single output stream.
        '''
        if(self.output_stream.is_closed()):
            return
        # Extracts input data sequentially from each input filter
        for input_iter in self.input_iterators:
            if input_iter.has_next():
                self.output_stream.append(next(input_iter))
                return
        # if not all of the input streams are closed we go on (return)
        for input_str in self.get_input_streams():
            if not input_str.is_closed():
                return
        # If we get here it means that all of the input streams are closed, hence we define the output as closed
        self.output_stream.close()
