from typing import Callable, Sequence, Mapping, Any
from .filter_layer import FilterLayer
from .stream import Stream


class FilterNet:
    '''
    Ordered collection of filter layers.

    Attributes:
        stream_dict : Mapping[str : Stream]
            Mapping of streams used by filters to read and write data.
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
        if layers == None:
            self.__layers = []
        else:
            self.__layers = layers
        self.stream_dict = dict()
        self.state_dict = dict()

    def add_layer(self, layer: FilterLayer):
        '''
        Appends a layer at the end of the current sequence of layers.

        Parameters:
            layer : FilterLayer
                Non-empty layer of filters.
        '''
        self.__layers.append(layer)

    def execute(self, source: Mapping[str, Stream], on_data_output: Callable = None):
        '''
        Works on the source streams with the given filter layers

        Parameter:
            source : Mapping[str : Streams]
                Source streams.
            on_data_output : Callable
                Function called everytime any filter from the last layer outputs something in any of its output streams.
        '''
        self.stream_dict.update(source)
        # Setup phase
        for filter_layer in self.__layers:
            for f in filter_layer:
                f.setup(self.__get_streams_by_names(f.get_inputs()),self.__get_streams_by_names(f.get_outputs()), self.state_dict)

        # Execute phase
        while(not self.__is_all_finished()):
            for filter_layer in self.__layers:
                for fil in filter_layer:
                    fil.execute()
        return self

    def streams(self) -> Mapping[str, Stream]:
        '''
        Retrieves the mapping of streams associated with their names.
        It's empty if execute() has never been called
        '''
        return self.stream_dict

    def state(self, key: str, default : Any) -> Any:
        return self.state_dict.get(key,default)

    def __is_all_finished(self) -> bool:
        '''
        Checks if the last filter layer's filters' output streams are flagged as closed.
        All streams must be initialised inside the self.stream_dict class variable.
        '''

        for l_filter in self.__layers[len(self.__layers) - 1]:
            for ouput_stream_name in l_filter.get_outputs():
                # If even one of the output streams is not closed, then continue execution
                if not self.stream_dict[ouput_stream_name].is_closed():
                    return False
        return True

    def __get_streams_by_names(self, names: Sequence[str]) -> Sequence[Stream]:
        '''
        Retrieves the required streams as a sequence.
        If a stream is not found it's initialised and stored into the dict.
        '''
        streams = []
        for name in names:
            # setdefault(key, default) returns value if key is present, default otherwise and stores key : default in the dict
            streams.append(self.stream_dict.setdefault(name, Stream()))
        return streams