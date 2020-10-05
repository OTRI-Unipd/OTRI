from typing import Sequence, Mapping, Any, List
from .stream import Stream


class Filter:
    '''
    Class that defines an atom manipulation filter.
    To change the order of input streams inspection override the _input_check_order method.
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
        if self._are_outputs_closed():
            self._on_outputs_closed()
            return

        self._has_outputted = False
        # Extracts input data sequentially from each input filter
        for i in self._input_check_order():
            if self.__input_iters[i].has_next():
                self._on_data(next(self.__input_iters[i]), i)
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

    def _get_inputs(self) -> Sequence[Stream]:
        '''
        Retrieves all of the input streams.
        '''
        return self.__input_streams

    def _get_output(self, index: int = 0) -> Stream:
        '''
        Retrieves one specific output stream.
        '''
        return self.__output_streams[index]

    def _get_outputs(self) -> Sequence[Stream]:
        '''
        Retrieves all of the output streams.
        '''
        return self.__output_streams

    def _get_in_iter(self, index: int = 0) -> Stream:
        '''
        Retrieves one specific input stream's iterator.
        '''
        return self.__input_iters[index]

    def _get_out_iter(self, index: int = 0) -> Stream:
        '''
        Retrieves one specific output stream's iterator.
        '''
        return self.__output_iters[index]

    def _pop_data(self, index: int = 0) -> Any:
        '''
        Pops one piece of data from an input.
        '''
        return next(self.__input_iters[index])

    def _push_data(self, data: Any, index: int = 0):
        '''
        Pushes one piece of data in an output.
        '''
        self._has_outputted = True
        if self.__output_streams[index] is not None:
            self.__output_streams[index].append(data)

    def _are_outputs_closed(self):
        for stream in self.__output_streams:
            if not stream.is_closed():
                return False
        return True

    # ? MANDATORY OVERRIDE ---

    def _on_data(self, data: Any, index: int):
        '''
        Called when one of the inputs has some data and it's been popped.
        Input could be still open or closed.

        Parameters:
            data : Any
                Popped data from an input.
            index : int
                The index of the input the data has been popped from.
        '''
        raise NotImplementedError("Filter is an abstract class, please implement this method.")

    # ? OPTIONAL OVERRIDE ---

    def _on_outputs_closed(self):
        '''
        Called when all of the outputs have already been closed.
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
        return range(0, len(self.__input_iters))


class ParallelFilter(Filter):

    '''
    Alternative for the Filter, it waits for all open inputs to have some data ready, then pops
    one atom from each of them and feeds the sequence to `_on_data(data, index)`.
    '''

    def __init__(self, inputs: Sequence[str], outputs: Sequence[str]):
        '''
        Similar to the parent class. Only difference is the inputs and outputs number must be the same.
        See `Filter` for details.

        Raises:
            ValueError : When the number of inputs and outputs is different.
        '''
        # Check same amount of inputs and outputs.
        if len(inputs) != len(outputs):
            raise ValueError("The number of input and output Streams must be the same.")

        super().__init__(inputs, outputs, len(inputs), len(outputs))

    def execute(self):
        '''
        This method gets called by the FilterNet when the filter has to manipulate data.
        It should:
        - Pop a single piece of data from one of the input streams.
        - Elaborate it and optionally update its state.
        - If it has produced something, push it into the output streams.
        '''
        # Checks if the filter has finished
        if self._are_outputs_closed():
            self._on_outputs_closed()
            return

        # Input streams that have an atom ready.
        input_indexes = list()

        self._has_outputted = False
        # Find open inputs with ready data
        for i in self._input_check_order():
            # Mark open Stream.
            if self._get_in_iter(i).has_next():
                input_indexes.append(i)
            # If no item and not closed wait. If closed ignore and continue.
            elif not self._get_input(i).is_closed():
                # Return if a Stream is not ready.
                self._on_inputs_empty()
                return

        # If we're here, all inputs are closed or have an atom.
        # We run _on_data normally.
        if input_indexes:
            input_atoms = [self._pop_data(i) for i in input_indexes]
            self._on_data(input_atoms, input_indexes)
            return

        # Checks if any of the input streams is still open
        for input_stream in self._get_inputs():
            if not input_stream.is_closed():
                self._on_inputs_empty()
                return

        # No more data and all of the inputs closed
        self._on_inputs_closed()

    def _on_data(self, data: List[Mapping], indexes: List[int]):
        '''
        This method is different from the Filter superclass.
        It accepts a list of atoms (one from each input) and a list of indexes (the input streams
        that are still open).

        Parameters:
            data : List[Mapping]
                The list of atoms from the inputs, one for each of the inputs that are still open.

            indexes : List[int]
                The indexes of the Streams from which the atoms come from.
        '''
        raise NotImplementedError(
            "ParallelFilter is an abstract class, please implement this method."
        )
