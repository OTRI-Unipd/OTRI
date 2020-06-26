from ..filter import Filter, Stream, Collection
from typing import Callable


class GenericFilter(Filter):
    '''
    Applies a given Callable to all input atoms and outputs them in the output Stream.
    Inputs: a single Stream.
    Outputs: a single Stream.
    '''

    def __init__(self, source_stream: Stream, operation: Callable):
        '''
        Parameters:
            source_stream : Stream
                A single Stream on which the operation will be applied
            operation : Callable
                The operation that must be applied to the input Stream.
        '''
        super().__init__(
            input_streams=[source_stream],
            input_streams_count=1,
            output_streams_count=1
        )
        self.__operation = operation
        self.__source_iter = source_stream.__iter__()

    def execute(self):
        '''
        Method called when a single step in the filtering must be taken.
        If the input stream has another item, the operation init parameter will be
        applied to it, and its return value will be put in the output stream.
        '''
        if self.get_output_stream(0).is_closed():
            return
        if self.__source_iter.has_next():
            item = self.__source_iter.__next__()
            self.get_output_stream(0).append(self.__operation(item))
        elif self.get_input_stream(0).is_closed():
            # Closed input -> Close outputs
            self.get_output_stream(0).close()
            return
