from ..filter import Filter, Any
from typing import Callable


class GenericFilter(Filter):
    '''
    Applies a given Callable to all data that passes through.

    Inputs:
        Single stream.
    Outputs:
        Single stream.
    '''

    def __init__(self, inputs: str, outputs: str, operation: Callable):
        '''
        Parameters:
            inputs : str
                A single stream name on which the operation will be applied.
            outputs: str
                The desired output stream name.
            operation : Callable
                The operation that must be applied to the data of the input stream.
        '''
        super().__init__(
            inputs=[inputs],
            outputs=[outputs],
            input_count=1,
            output_count=1
        )
        self.__operation = operation

    def _on_data(self, data: Any, index: int):
        '''
        Applies the operation on the atom then pushes it into the output
        '''
        self._push_data(self.__operation(data))
