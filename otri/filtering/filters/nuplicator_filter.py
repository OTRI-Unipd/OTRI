from ..filter import Filter, Stream, Sequence
import copy


class NUplicatorFilter(Filter):
    '''
    N-uplicates the input stream. Placing a copy of the input in each output filter.

    Input: 
        Single stream.
    Outputs:
        Any number of streams.
    '''

    def __init__(self, input: str, output: Sequence[str], deep_copy: bool = True):
        '''
        Parameters:
            input : Sequence[str]
                Name for input stream that is n-uplicated.
            output : str
                Name for output streams.
            deep_copy : bool = False
                Whether the items from the input stream should be deep copies or shallow copies
        '''
        super().__init__(
            input=[input],
            output=output,
            input_count=1,
            output_count=len(output)
        )
        self.__copy = copy.deepcopy if deep_copy else copy.copy

    def execute(self, inputs : Sequence[Stream], outputs : Sequence[Stream]):
        '''
        Method called when a single step in the filtering must be taken.
        If the input stream has another item, copy it to all output streams.
        If the input stream has no other item and got closed, then we also close
        the output streams.

        Parameters:
            inputs, outputs : Sequence[Stream]
                Ordered sequence containing the required input/output streams gained from the FilterList.
        '''
        if outputs[0].is_closed():
            return
        if iter(inputs[0]).has_next():
            item = next(iter(inputs[0]))
            for output in outputs:
                output.append(self.__copy(item))
        elif inputs[0].is_closed():
            # Closed input -> Close outputs
            for output in outputs:
                output.close()
