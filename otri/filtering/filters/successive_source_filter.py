from ..filter import Filter, StreamIter, Stream, Collection
from ..stream import Stream


class SuccessiveSourceFilter(Filter):
    '''
    Pops element from a single input stream.
    Can only be used as first filter of a filter list.
    '''

    def __init__(self, source_streams: Collection[Stream]):
        '''
    	Parameters:
            source_streams : Collection[Stream]
                Collection of source streams of the filter list.
        '''
        super().__init__(input_streams=source_streams, input_streams_count=len(source_streams), output_streams_count=1)
        # caching iterators to avoid creating a new one every time execute() is called
        self.origin_iterators = [x.__iter__() for x in source_streams]
        self.output_stream = self.get_output_stream(0)

    def execute(self):
        '''
        Pops elements from the input streams sequentially (all of stream 0 then all of stream 1 and so on) and places them into the single output stream.
        The output stream is flagged as 'is_finished' when all of the input streams have no more elements.
        '''
        
        for iterator in self.origin_iterators:
            if(iterator.has_next()):
                value = next(iterator)
                self.output_stream.insert(value)
                return
        # If we get here it means that all of the input streams contained no elements, hence we declare the finished state
        self.output_stream.close()

