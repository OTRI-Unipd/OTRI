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

    def setup(self, inputs: Sequence[Stream], outputs: Sequence[Stream], status: Mapping[str, Any]):
        '''
        Used to save references to streams and reset variables.
        Called once before the start of the execution in FilterList.

        Parameters:
            inputs, outputs : Sequence[Stream]
                Ordered sequence containing the required input/output streams gained from the FilterList.
            status : Mapping[str, Any]
                Dictionary containing statuses to output.
        '''
        self.__input = inputs[0]
        self.__input_iter = iter(inputs[0])
        self.__output = outputs[0]

    def execute(self):
        '''
        Performs given operations on keys of the item.
        '''
        if(self.__output.is_closed()):
            return

        if( self.__input_iter.has_next()):
            atom = next(self.__input_iter)
            for key in self.__keys_operations.keys():
                atom[key] = self.__keys_operations[key](atom[key])
            self.__output.append(atom)

        elif(self.__input.is_closed()):
            self.__output.close()
