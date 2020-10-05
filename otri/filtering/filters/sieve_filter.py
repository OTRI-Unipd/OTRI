from ..filter import Filter

from typing import Callable, Any


class SieveFilter(Filter):
    '''
    Outputs only data that returns True on the given operation.

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
                The operation determining wether data will be written or not.
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
        Writes the data to the output if the given operation returns True.
        '''
        if self.__operation(data):
            self._push_data(data)
