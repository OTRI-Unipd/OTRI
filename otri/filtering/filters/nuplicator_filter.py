from ..filter import Filter, Stream, Collection
import copy


class NUplicatorFilter(Filter):
    '''
    N-uplicates the input stream. Placing a copy of the input in each output filter.
    Inputs: a single Stream.
    Outputs: Any number of streams.
    '''

    def __init__(self, source_stream: Stream, output_streams_count: int, deep_copy: bool = False):
        '''
        Parameters:
            source_stream : Stream
                A single Stream that must be n-uplicated
            output_streams_count : int
                The number of output streams for this filter
            deep_copy : bool = False
                Whether the items from the input stream should be deep copies or shallow copies
        '''
        super().__init__(
            input_streams=[source_stream],
            input_streams_count=1,
            output_streams_count=output_streams_count
        )
        self.copy = copy.deepcopy if deep_copy else copy.copy
        self.source_iter = source_stream.__iter__()

    def execute(self):
        '''
        Method called when a single step in the filtering must be taken.
        If the input stream has another item, copy it to all output streams.
        If the input stream has no other item and got closed, then we also close
        the output streams.
        '''
        if self.source_iter.has_next():
            item = self.source_iter.__next__()
            for output in self.get_output_streams():
                output.append(self.copy(item))
        elif self.get_input_stream(0).is_closed():
            # Closed input -> Close outputs
            for output in self.get_output_streams():
                output.close()
            return
