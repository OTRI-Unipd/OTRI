from ..filter import Filter, Stream, Collection
from typing import Callable


class ValidatorFilter(Filter):
    '''
    Filters atoms based on a series of checks.
    Inputs: a single Stream.
    Outputs: a single Stream, containing only the items that pass the checks.
    '''

    def __init__(self, source_stream: Stream, checks: Collection[Callable]):
        '''
        Parameters:
            source_stream : Stream
                A single Stream on which the operation will be applied
            checks : Collection[Callable]
                The operations to apply to the Stream, must return a value
                that is valid as a boolean expression (i.e. bool, number, list...)
        '''
        super().__init__(
            input_streams=[source_stream],
            input_streams_count=1,
            output_streams_count=1
        )
        self.__checks = checks
        self.__source_iter = source_stream.__iter__()

    def execute(self):
        '''
        Method called when a single step in the filtering must be taken.
        If the input stream has another item and the item's result is True
        for al the checks, the item will be put in the output stream.
        '''
        if self.get_output_stream(0).is_closed():
            return
        if self.__source_iter.has_next():
            item = self.__source_iter.__next__()
            for c in self.__checks:
                if not c(item):
                    return
            self.get_output_stream(0).append(item)
        elif self.get_input_stream(0).is_closed():
            # Closed input -> Close outputs
            self.get_output_stream(0).close()
            return
