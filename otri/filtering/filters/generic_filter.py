from ..filter import Filter, Stream, Sequence, Mapping, Any
from typing import Callable


class GenericFilter(Filter):
    '''
    Applies a given Callable to all input data and outputs them in the output stream.
    
    Input:
        Single stream.
    Outputs:
        Single stream.
    '''

    def __init__(self, input: str, output: str, operation: Callable):
        '''
        Parameters:
            input : str
                A single stream name on which the operation will be applied.
            output : str
                The desired output stream name.
            operation : Callable
                The operation that must be applied to the data of the input stream.
        '''
        super().__init__(
            input=[input],
            output=[output],
            input_count=1,
            output_count=1
        )
        self.__operation = operation

    def execute(self, inputs : Sequence[Stream], outputs : Sequence[Stream], status: Mapping[str, Any]):
        '''
        Method called when a single step in the filtering must be taken.
        If the input stream has another item, the operation init parameter will be
        applied to it, and its return value will be put in the output stream.
        
        Parameters:
            inputs, outputs : Sequence[Stream]
                Ordered sequence containing the required input/output streams gained from the FilterList.
        '''
        if outputs[0].is_closed():
            return

        if iter(inputs[0]).has_next():
            item = next(iter(inputs[0]))
            outputs[0].append(self.__operation(item))
        elif inputs[0].is_closed():
            outputs[0].close()
