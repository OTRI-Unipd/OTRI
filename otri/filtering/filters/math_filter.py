from ..filter import Filter, Stream, Sequence, Any, Mapping
from typing import Callable


class MathFilter(Filter):
    '''
    Performs a give operation on keys of an item.
    Input:
        Single stream.
    Output:
        Single stream.
    '''

    def __init__(self, input: str, output: str, keys_operations: Mapping[str, Callable]):
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
            input=[input],
            output=[output],
            input_count=1,
            output_count=1)
        self.__keys_operations = keys_operations

    def execute(self, inputs : Sequence[Stream], outputs : Sequence[Stream], status: Mapping[str, Any]):
        '''
        Performs given operations on keys of the item.

        Parameters:
            inputs, outputs : Sequence[Stream]
                Ordered sequence containing the required input/output streams gained from the FilterList.
        '''
        if(outputs[0].is_closed()):
            return

        if(iter(inputs[0]).has_next()):
            atom = next(iter(inputs[0]))
            for key in self.__keys_operations.keys():
                atom[key] = self.__keys_operations[key](atom[key])
            outputs[0].append(atom)

        elif(inputs[0].is_closed()):
            outputs[0].close()
