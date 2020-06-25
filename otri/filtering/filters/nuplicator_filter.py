from ..filter import Filter, Stream, Sequence, Mapping, Any
import copy


class NUplicatorFilter(Filter):
    '''
    N-uplicates the input stream. Placing a copy of the input in each output filter.

    Input: 
        Single stream.
    Outputs:
        Any number of streams.
    '''

    def __init__(self, inputs: str, outputs: Sequence[str], deep_copy: bool = True):
        '''
        Parameters:
            inputs : str
                Name for input stream that is n-uplicated.
            outputs : Sequence[str]
                Name for output streams.
            deep_copy : bool = False
                Whether the items from the input stream should be deep copies or shallow copies
        '''
        super().__init__(
            inputs=[inputs],
            outputs=outputs,
            input_count=1,
            output_count=len(outputs)
        )
        self.__copy = copy.deepcopy if deep_copy else copy.copy

    def setup(self, inputs : Sequence[Stream], outputs : Sequence[Stream], state: Mapping[str, Any]):
        '''
        Used to save references to streams and reset variables.
        Called once before the start of the execution in FilterList.

        Parameters:
            inputs, outputs : Sequence[Stream]
                Ordered sequence containing the required input/output streams gained from the FilterList.
            state : Mapping[str, Any]
                Dictionary containing states to output.
        '''
        self.__input = inputs[0]
        self.__input_iter = iter(inputs[0])
        self.__outputs = outputs

    def execute(self):
        '''
        Method called when a single step in the filtering must be taken.
        If the input stream has another item, copy it to all output streams.
        If the input stream has no other item and got closed, then we also close
        the output streams.
        '''
        if self.__outputs[0].is_closed():
            return
        if self.__input_iter.has_next():
            item = next(self.__input_iter)
            for output in self.__outputs:
                output.append(self.__copy(item))
        elif self.__input.is_closed():
            # Closed input -> Close outputs
            for output in self.__outputs:
                
                output.close()
