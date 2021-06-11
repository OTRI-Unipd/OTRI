from typing import Sequence, Mapping, Any
from .stream import Stream


class Filter:
    '''
    Class that defines an atom manipulation filter. 
    To change the order of input streams inspection override the _input_check_order method.
    '''

    def __init__(self, inputs: Sequence[str], outputs: Sequence[str], input_count: int = 0, output_count: int = 0):
        '''
        Parameters:\n
            inputs : Sequence[str]
                Name for input streams.\n
            outputs : Sequence[str]
                Name for output streams.\n

            If there are multiple streams for input or output the filter must explicit the right order for the user to name them correctly.
            Both input and output streams will be gathered/saved inside the FilterNet's dictionary of streams.

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
        if(len(inputs) != input_count):
            raise ValueError("this filter takes {} input streams, {} given".format(
                input_count, len(inputs)))
        if(len(outputs) != output_count):
            raise ValueError("this filter takes {} output streams, {} given".format(
                output_count, len(outputs)))
        self.output_names = outputs
        self.input_names = inputs
        self._has_outputted = False

    def setup(self, inputs: Sequence[Stream], outputs: Sequence[Stream], state: Mapping[str, Any]):
        '''
        Allows the filter to save references to streams and reset its variables before the execution.
        Parameters:
            inputs, outputs : Sequence[Stream]
                Ordered sequence containing the required input/output streams gained from the FilterNet.
            state : Mapping[str, Any]
                Dictionary containing states to output.
        '''
        # Save references to streams
        self.__input_streams = inputs
        self.__output_streams = outputs
        self.__state = state

    def execute(self):
        '''
        This method gets called by the FilterNet when the filter has to manipulate data.
        It should:
        - Pop a single piece of data from one of the input streams.
        - Elaborate it and optionally update its state.
        - If it has produced something, push it into the output streams.
        '''
        # Checks if the filter has finished
        if self.__are_outputs_closed():
            self._on_outputs_closed()
            return

        self._has_outputted = False
        # Extracts input data sequentially from each input filter
        for i in self._input_check_order():
            if self.__input_streams[i].has_next():
                self._on_data(self.__input_streams[i].pop(), i)
                return

        # Checks if any of the input streams is still open
        for input_stream in self.__input_streams:
            if not input_stream.is_closed():
                self._on_inputs_empty()
                return

        # No more data and all of the inputs closed
        self._on_inputs_closed()

    def _get_inputs(self) -> Sequence[Stream]:
        '''
        Retrieves all of the input streams.
        '''
        return self.__input_streams

    def _get_outputs(self) -> Sequence[Stream]:
        '''
        Retrieves all of the output streams.
        '''
        return self.__output_streams

    def _push_data(self, data: Any, index: int = 0):
        '''
        Pushes one piece of data in one of the output streams.
        '''
        self._has_outputted = True
        self.__output_streams[index].push(data)

    # OVERRIDABLE METHODS

    def _on_outputs_closed(self):
        '''
        Called when all of the outputs have already been closed.
        '''
        pass

    def _on_data(self, data: Any, index: int):
        '''
        Called when one of the inputs has some data and it's been popped.
        Input could be still open or could be closed.\n

        Parameters:\n
            data : Any
                Popped data from an input.\n
            index : int
                The index of the input the data has been popped from.\n
        '''
        pass

    def _on_inputs_empty(self):
        '''
        All of the inputs have no data, but not all of them are closed.
        '''
        pass

    def _on_inputs_closed(self):
        '''
        All of the inputs are closed and no more data is available.
        The filter should empty itself and close all of the output streams.
        '''
        for out_stream in self.__output_streams:
            if not out_stream.is_closed():
                out_stream.close()

    def _input_check_order(self) -> Sequence[int]:
        '''
        Defines the order for the inputs to be checked.
        By default its just an ordered sequence from 0 to len(inputs).
        '''
        return range(0, len(self.__input_streams))

    # PRIVATE METHODS

    def __are_outputs_closed(self):
        for stream in self.__output_streams:
            if not stream.is_closed():
                return False
        return True
