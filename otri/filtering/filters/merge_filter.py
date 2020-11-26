from ..filter import Filter, Queue, Sequence, Mapping, Any


class SequentialMergeFilter(Filter):
    '''
    Sequentially merges elements from multiple queues into one single output.

    Inputs:
        Multiple queues.
    Outputs:
        A single queue containing data read sequentially (all of queue 1, then all of queue 2 and so on).
    '''

    def __init__(self, inputs: Sequence[str], outputs: str):
        '''
        Parameters:
             input : Sequence[str]
                Name for input queues.
            output : str
                Name for output queue.
        '''
        super().__init__(
            inputs=inputs,
            outputs=[outputs],
            input_count=len(inputs),
            output_count=1)

    def _on_data(self, data, index):
        '''
        Places the passed atom in the only output.
        '''
        self._push_data(data)

    def _input_check_order(self) -> Sequence:
        '''
        Defines the order for the inputs to be checked.
        We choose it to be sequential.
        '''
        return range(0, len(self.input_names))
