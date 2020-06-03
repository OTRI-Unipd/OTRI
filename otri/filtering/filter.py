from typing import Sequence, Mapping, Any
from .stream import Stream


class Filter:
    '''
    Abstract class that defines an atom manipulation filter.

    Attributes:
        input : Sequence[str]
            Name for input streams. If there are multiple streams the filter must define the right order.
            Streams will be gathered inside the FilterList's dictionary of streams.
        output : Sequence[str]
            Name for output streams,
    '''

    def __init__(self, input: Sequence[str], output: Sequence[str], input_count: int = 0, output_count: int = 0):
        '''
        Parameters:
            input : Sequence[str]
                Name for input streams.
            output : Sequence[str]
                Name for output streams.

            If there are multiple streams for input or output the filter must explicit the right order for the user to name them correctly.
            Both input and output streams will be gathered/saved inside the FilterList's dictionary of streams.

            Reserved to sub-classes:
                input_count : int
                    The number of input streams that the filter uses.
                output_count : int
                    The number of output streams that the filter uses.

                Both these numbers will be used to ensure that the filter gets the right amount of parameters.
        Raises:
            ValueError
                if the given input or output sequence has a different cardinality than expected.
        '''
        if(len(input) != input_count):
            raise ValueError("this filter takes {} input streams, {} given".format(
                input_count, len(input)))
        if(len(output) != output_count):
            raise ValueError("this filter takes {} output streams, {} given".format(
                output_count, len(output)))
        self.__output = output
        self.__input = input

    def setup(self, inputs: Sequence[Stream], outputs: Sequence[Stream], status: Mapping[str, Any] = None):
        '''
        Allows the filter to save references to streams and reset its variables.
        Parameters:
            inputs, outputs : Sequence[Stream]
                Ordered sequence containing the required input/output streams gained from the FilterList.
            statuss : Mapping[str, Any]
                Dictionary containing statuses to output.
        '''
        raise NotImplementedError("filter sub-classes must override setup method")

    def execute(self):
        '''
        This method gets called by the FilterList when the filter has to manipulate data.
        It should:
        - Pop a single piece of data from one of the input streams.
        - Elaborate it and optionally update its state.
        - If it has produced something, push it into the output streams.
        '''
        raise NotImplementedError("filter sub-classes must override execute method")

    def get_input(self) -> Sequence[str]:
        '''
        Retrieve the input streams names.
        '''
        return self.__input

    def get_output(self) -> Sequence[str]:
        '''
        Retrieve the output streams names.
        '''
        return self.__output
