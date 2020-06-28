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

    def setup(self, inputs : Sequence[Stream], outputs : Sequence[Stream], state: Mapping[str, Any]):
        '''
        Used to save references to streams and reset variables.
        Called once before the start of the execution in FilterNet.
        
        Parameters:
            inputs, outputs : Sequence[Stream]
                Ordered sequence containing the required input/output streams gained from the FilterNet.
            state : Mapping[str, Any]
                Dictionary containing states to output.
        '''
        self.__input = inputs[0]
        self.__input_iter = iter(inputs[0])
        self.__output = outputs[0]

    def execute(self):
        '''
        Method called when a single step in the filtering must be taken.
        If the input stream has another item, the operation init parameter will be
        applied to it, and its return value will be put in the output stream.
        '''
        if self.__output.is_closed():
            return

        if self.__input_iter.has_next():
            item = next(self.__input_iter)
            self.__output.append(self.__operation(item))
        elif self.__input.is_closed():
            self.__output.close()
