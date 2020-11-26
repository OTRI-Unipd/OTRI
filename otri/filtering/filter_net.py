from typing import Callable, Sequence, Mapping, Any
from .filter_layer import FilterLayer
from .queue import Queue, LocalQueue


class FilterNet:
    '''
    Ordered collection of filter layers.

    Attributes:
        queue_dict : Mapping[str : Queue]
            Mapping of queues used by filters to read and write data.
        stats_dict : Mapping[str : Any]
            Mapping of states updated by filters.
    '''

    def __init__(self, layers: Sequence[FilterLayer] = None):
        '''
        Parameters:
            layers : Sequence[FilterLayer]
                Ordered sequence of layers that this list uses.
                All filters must not be empty.
        '''
        if layers is None:
            self.__layers = []
        else:
            self.__layers = layers
        self.queue_dict = dict()
        self.state_dict = dict()

    def add_layer(self, layer: FilterLayer):
        '''
        Appends a layer at the end of the current sequence of layers.

        Parameters:
            layer : FilterLayer
                Non-empty layer of filters.
        '''
        self.__layers.append(layer)

    def execute(self, source: Mapping[str, Queue], on_data_output: Callable = None):
        '''
        Works on the source queues with the given filter layers

        Parameter:
            source : Mapping[str : Queues]
                Source queues.
            on_data_output : Callable
                Function called everytime any filter from the last layer outputs something in any of its output queues.
        '''
        self.queue_dict.update(source)
        # Setup phase
        for filter_layer in self.__layers:
            for f in filter_layer.filters:
                f.setup(self.__get_queues_by_names(f.input_names),self.__get_queues_by_names(f.output_names), self.state_dict)

        # Execute phase
        layer_index = 0
        layer = None
        while(True):
            layer = self.__layers[layer_index]
            # Execute all the filters of the layer
            for fil in layer.filters:
                fil.execute()
            # Check if it's finished
            if layer_index >= len(self.__layers) - 1:
                # Call on_data_output if the last layer has outputted something
                if on_data_output is not None and layer.has_outputted():
                    for f in layer.filters:
                        if f._has_outputted:
                            on_data_output()
                if self.__is_all_finished():
                    break
            # Ask the policy for the new layer index
            layer_index += layer.call_policy()
            if(layer_index >= len(self.__layers)):
                layer_index = 0
            elif(layer_index < 0):
                layer_index = 0

        # Returns self for method concatenation
        return self

    def queues(self) -> Mapping[str, Queue]:
        '''
        Retrieves the mapping of queues associated with their names.
        It's empty if execute() has never been called.
        '''
        return self.queue_dict

    def state(self, key: str, default: Any) -> Any:
        return self.state_dict.get(key, default)

    def __is_all_finished(self) -> bool:
        '''
        Checks if the last filter layer's filters' output queues are flagged as closed.
        All queues must be initialised inside the self.queue_dict class variable.
        '''

        for l_filter in self.__layers[len(self.__layers) - 1].filters:
            for output_queue_name in l_filter.output_names:
                # If even one of the output queues is not closed, then continue execution
                if output_queue_name is not None and not self.queue_dict[output_queue_name].is_closed():
                    return False
        return True

    def __get_queues_by_names(self, names: Sequence[str]) -> Sequence[Queue]:
        '''
        Retrieves the required queues as a sequence.
        If a queue is not found it's initialised as LocalQueue and stored into the queues dict.
        '''
        queues = []
        for name in names:
            # setdefault(key, default) returns value if key is present, default otherwise and stores key : default in the dict
            queues.append(self.queue_dict.setdefault(name, LocalQueue(elements=None, closed=False)))
        return queues


# Policies
'''
Policies:
A policy is a method that returns a number that represents how many layers skip forward (or backward if the number is negative)
when  the current layer has been executed.
If the layer index exceeds the number of layers the next layer is the first one.
If the layer index is smaller than 0 the next layer is the first one.
'''


def EXEC_AND_PASS(layer: FilterLayer):
    return 1


def EXEC_UNTIL_FINISHED(layer: FilterLayer):
    for f in layer.filters:
        for output_queue in f._get_outputs():
            # If even one of the output queues is not closed, then continue execution of the current layer
            if not output_queue.is_closed():
                return 0
    return 1


def EXEC_UNTIL_OUTPUT(layer: FilterLayer):
    if layer.has_outputted():
        return 1
    return 0


def BACK_IF_NO_OUTPUT(layer: FilterLayer):
    if layer.has_outputted() or layer.has_finished():
        # Keep executing if it has outputted anything
        return 1
    return -1


def BACK_IF_OUTPUT(layer: FilterLayer):
    if layer.has_outputted():
        return -1
    return 1
