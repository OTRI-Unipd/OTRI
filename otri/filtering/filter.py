from typing import Sequence, Mapping, Any
from .stream import Stream


class Filter:
    '''
    Abstract class that defines an atom manipulation filter.
    '''

    def __init__(self, inputs: Sequence[str], outputs: Sequence[str], input_count: int = 0, output_count: int = 0):
        '''
        Parameters:
            inputs : Sequence[str]
                Name for input streams.
            outputs : Sequence[str]
                Name for output streams.

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
        self.__output_names = outputs
        self.__input_names = inputs

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

        # Save references to iterators
        self.__input_iters = list()
        self.__output_iters = list()
        for stream in inputs:
            self.__input_iters.append(iter(stream))
        for stream in outputs:
            self.__output_iters.append(iter(stream))

    def execute(self):
        '''
        This method gets called by the FilterNet when the filter has to manipulate data.
        It should:
        - Pop a single piece of data from one of the input streams.
        - Elaborate it and optionally update its state.
        - If it has produced something, push it into the output streams.
        '''
        # Checks if the filter has finished
        if __are_outputs_closed():
            self._on_outputs_closed()
            return

        # Extracts input data sequentially from each input filter
        for i in self._input_check_order():
            if self.input_iter[i].has_next():
                self._on_data(next(self.input_iter[i]),i)
                return

        # Checks if any of the input streams is still open
        for input_stream in self.__input_streams:
            if not input_stream.is_closed():
                self._on_inputs_empty()
                return
        
        # No more data and all of the inputs closed
        self._on_inputs_closed()


    def get_input_names(self) -> Sequence[str]:
        '''
        Retrieve the input streams names.
        '''
        return self.__input_names

    def get_output_names(self) -> Sequence[str]:
        '''
        Retrieve the output streams names.
        '''
        return self.__output_names

    def _get_input(self, index: int = 0) -> Stream:
        '''
        Retrieves one specific input stream.
        '''
        return self.__input_streams[index]

    def _get_output(self, index: int = 0) -> Stream:
        '''
        Retrieves one specific output stream.
        '''
        return self.__output_streams[index]

    def _get_in_iter(self, index: int = 0) -> Stream:
        '''
        Retrieves one specific input stream's iterator.
        '''
        return self._input_iters[index]

    def _get_out_iter(self, index: int = 0) -> Stream:
        '''
        Retrieves one specific output stream's iterator.
        '''
        return self._output_iters[index]

    def _pop_data(self, index: int = 0) -> Any:
        '''
        Pops one piece of data from an input.
        '''
        return next(self.__input_iters[index])

    def _push_data(self, data: Any, index: int = 0):
        '''
        Pushes one piece of data in an output.
        '''
        self._output_iters[index].append(data)

    # OVERRIDABLE METHODS

    def _on_outputs_closed(self):
        '''
        Called when all of the outputs have already been closed.
        '''
        pass

    def _on_data(self, data : Any, index : int):
        '''
        Called when one of the inputs has some data and it's been popped.
        Input could be still open or closed.

        Parameters:
            data : Any
                Popped data from an input.
            index : int
                The index of the input the data has been popped from.
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
            out_stream.close()
    
    def _input_check_order(self)->Sequence:
        '''
        Defines the order for the inputs to be checked.
        By default its just an ordered sequence from 0 to len(inputs).
        '''
        return range(0, len(self.__input_iters))

    # PRIVATE METHODS

    def __are_outputs_closed(self):
        for stream in self.__output_streams:
            if not stream.is_closed():
                return False
        return True
