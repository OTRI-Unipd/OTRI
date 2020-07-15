from ..filter import Filter, Stream, Sequence, Mapping, Any


class SequentialMergeFilter(Filter):
    '''
    Sequentially merges elements from multiple streams into one single output.

    Inputs:
        Multiple streams.
    Outputs:
        A single stream containing data read sequentially (all of stream 1, then all of stream 2 and so on).
    '''

    def __init__(self, inputs: Sequence[str], outputs: str):
        '''
        Parameters:
             input : Sequence[str]
                Name for input streams.
            output : str
                Name for output stream.
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
        return range(0, len(self.get_input_names()))
