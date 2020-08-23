from ..filter import Filter, Stream, Sequence, Mapping, Any
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


class MultipleGenericFiler(Filter):
    '''
    Applies a given Callable passing an element per stream as parameters, then outputs a single element in the only output stream.\n

    Inputs:
        Multiple streams.\n
    Outputs:
        A single stream.
    '''

    def __init__(self, inputs: Sequence[str], outputs: str, operation: Callable):
        '''
        Parameters:\n
            inputs : Sequence[str]
                Stream names on which the operation will be applied.\n
            outputs: str
                The desired output stream name.\n
            operation : Callable
                The operation that must be applied to the data of the input stream. The parameter must be a single sequence of elements.\n
        '''
        super().__init__(
            inputs=inputs,
            outputs=[outputs],
            input_count=len(inputs),
            output_count=1
        )
        self.__inputs_len = len(inputs)
        self.__operation = operation

    def setup(self, inputs: Sequence[Stream], outputs: Sequence[Stream], state: Mapping[str, Any]):
        '''
        Used to save references to streams and reset variables.\n
        Called once before the start of the execution in FilterNet.\n

        Parameters:\n
            inputs, outputs : Sequence[Stream]
                Ordered sequence containing the required input/output streams gained from the FilterNet.\n
            state : Mapping[str, Any]
                Dictionary containing states to output.\n
        '''
        # Call superclass setup
        super().setup(inputs, outputs, state)
        self.__buffer = [None] * self.__inputs_len
        self.__next_stream = 0

    def _on_data(self, data: Any, index: int):
        # Save element in buffer
        self.__buffer[index] = data
        # Switch stream to the next one
        self.__next_stream = (index + 1) % self.__inputs_len
        # Apply the operation only if we have at least an element per stream
        if self.__is_ready():
            self._push_data(self.__operation(self.__buffer))
            self.__empty_buffer()

    def __is_ready(self) -> bool:
        '''
        Checks if there's  an element per stream.
        '''
        for element in self.__buffer:
            if element is None:
                return False
        return True

    def __empty_buffer(self):
        '''
        Sets all buffer elements to None.
        '''
        for i in range(len(self.__buffer)):
            self.__buffer[i] = None

    def _input_check_order(self) -> Sequence[int]:
        '''
        Defines the order for the inputs to be checked.
        '''
        return [self.__next_stream]
