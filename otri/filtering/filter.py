from typing import Sequence, Mapping, Any
from .queue import Queue


class Filter:
    '''
    Class that defines an atom manipulation filter. 
    To change the order of input queues inspection override the _input_check_order method.
    '''

    def __init__(self, inputs: Sequence[str], outputs: Sequence[str], input_count: int = 0, output_count: int = 0):
        '''
        Parameters:\n
            inputs : Sequence[str]
                Name for input queues.\n
            outputs : Sequence[str]
                Name for output queues.\n

            If there are multiple queues for input or output the filter must explicit the right order for the user to name them correctly.
            Both input and output queues will be gathered/saved inside the FilterNet's dictionary of queues.

            Reserved to sub-classes:
                input_count : int
                    The number of input queues that the filter uses.
                output_count : int
                    The number of output queues that the filter uses.

                Both these numbers will be used to ensure that the filter gets the right amount of parameters.
        Raises:
            ValueError
                if the given input or output sequence has a different cardinality than expected.
        '''
        if(len(inputs) != input_count):
            raise ValueError("this filter takes {} input queues, {} given".format(
                input_count, len(inputs)))
        if(len(outputs) != output_count):
            raise ValueError("this filter takes {} output queues, {} given".format(
                output_count, len(outputs)))
        self.output_names = outputs
        self.input_names = inputs
        self._has_outputted = False

    def setup(self, inputs: Sequence[Queue], outputs: Sequence[Queue], state: Mapping[str, Any]):
        '''
        Allows the filter to save references to queues and reset its variables before the execution.
        Parameters:
            inputs, outputs : Sequence[Queue]
                Ordered sequence containing the required input/output queues gained from the FilterNet.
            state : Mapping[str, Any]
                Dictionary containing states to output.
        '''
        # Save references to queues
        self.__input_queues = inputs
        self.__output_queues = outputs
        self.__state = state

    def execute(self):
        '''
        This method gets called by the FilterNet when the filter has to manipulate data.
        It should:
        - Pop a single piece of data from one of the input queues.
        - Elaborate it and optionally update its state.
        - If it has produced something, push it into the output queues.
        '''
        # Checks if the filter has finished
        if self.__are_outputs_closed():
            self._on_outputs_closed()
            return

        self._has_outputted = False
        # Extracts input data sequentially from each input filter
        for i in self._input_check_order():
            if self.__input_queues[i].has_next():
                self._on_data(self.__input_queues[i].pop(), i)
                return

        # Checks if any of the input queues is still open
        for input_queue in self.__input_queues:
            if not input_queue.is_closed():
                self._on_inputs_empty()
                return

        # No more data and all of the inputs closed
        self._on_inputs_closed()

    def _get_inputs(self) -> Sequence[Queue]:
        '''
        Retrieves all of the input queues.
        '''
        return self.__input_queues

    def _get_outputs(self) -> Sequence[Queue]:
        '''
        Retrieves all of the output queues.
        '''
        return self.__output_queues

    def _push_data(self, data: Any, index: int = 0):
        '''
        Pushes one piece of data in one of the output queues.
        '''
        self._has_outputted = True
        self.__output_queues[index].push(data)

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
        The filter should empty itself and close all of the output queues.
        '''
        for out_queue in self.__output_queues:
            if not out_queue.is_closed():
                out_queue.close()

    def _input_check_order(self) -> Sequence[int]:
        '''
        Defines the order for the inputs to be checked.
        By default its just an ordered sequence from 0 to len(inputs).
        '''
        return range(0, len(self.__input_queues))

    # PRIVATE METHODS

    def __are_outputs_closed(self):
        for queue in self.__output_queues:
            if not queue.is_closed():
                return False
        return True
