from ..filter import Filter, Queue, Sequence, Mapping, Any
import copy


class NUplicatorFilter(Filter):
    '''
    N-uplicates the input queue. Placing a copy of the input in each output filter.

    Inputs: 
        Single queue.
    Outputs:
        Any number of queues.
    '''

    def __init__(self, inputs: str, outputs: Sequence[str], deep_copy: bool = True):
        '''
        Parameters:
            inputs : str
                Name for input queue that is n-uplicated.
            outputs : Sequence[str]
                Name for output queues.
            deep_copy : bool = False
                Whether the items from the input queue should be deep copies or shallow copies
        '''
        super().__init__(
            inputs=[inputs],
            outputs=outputs,
            input_count=1,
            output_count=len(outputs)
        )
        self.__copy = copy.deepcopy if deep_copy else copy.copy

    def _on_data(self, data, index):
        '''
        Copies reference or deep copies atoms in multiple outputs.
        '''
        for i in range(len(self.output_names)):
            self._push_data(self.__copy(data), index = i)
