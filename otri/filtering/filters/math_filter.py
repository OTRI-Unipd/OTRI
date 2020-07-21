from ..filter import Filter, Stream, Sequence, Any, Mapping
from typing import Callable


class MathFilter(Filter):
    '''
    Performs a give operation on keys of an item.

    Inputs:
        Single stream.
    Outputs:
        Single stream.
    '''

    def __init__(self, inputs: str, outputs: str, keys_operations: Mapping[str, Callable]):
        '''
        Parameters:
            input : str
                A single stream name on which the operation will be applied.
            output : str
                The desired output stream name.
            keys_operations : Mapping[str : Callable]
                Collection of keys whom values will be summed for the given constant.
        '''
        super().__init__(
            inputs=[inputs],
            outputs=[outputs],
            input_count=1,
            output_count=1)
        self.__keys_operations = keys_operations

    def _on_data(self, data, index):
        '''
        Applies the given math to those values that match the given keys in the atom.
        '''
        for key in self.__keys_operations.keys():
            data[key] = self.__keys_operations[key](data[key])
        self._push_data(data)
