from typing import Callable, Sequence, Mapping, Any
from .filter_layer import FilterLayer
from .stream import Stream


class FilterList:
    '''
    Ordered collection of filter layers.

    Attributes:
        stream_dict : Mapping[str : Stream]
            Mapping of streams used by filters to read and write data.
        stats_dict : Mapping[str : Any]
            Mapping of statuses updated by filters.
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
        self.status_dict = dict()

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
                Function called everytime a filter from the last layer outputs something in any of its output layers.
        '''
        self.stream_dict.update(source)
        filter_streams = self.__filter_streams_dict()

        while(not self.__is_all_finished()):
            for filter_layer in self.__layers:
                for fil in filter_layer:
                    fil.execute(
                        inputs=filter_streams[fil]['inputs'],
                        outputs=filter_streams[fil]['outputs'],
                        status=self.status_dict
                    )
        return self

    def streams(self) -> Mapping[str, Stream]:
        return self.stream_dict

    def status(self, key: str, default : Any) -> Any:
        return self.status_dict.get(key,default)

    def __is_all_finished(self) -> bool:
        '''
        Checks if the last filter layer's filters' output streams are flagged as closed.
        '''
        for filter in self.__layers[len(self.__layers) - 1]:
            for ouput_stream_name in filter.get_output():
                # If even one of the output streams is not closed, then continue execution
                if not self.stream_dict.setdefault(ouput_stream_name, Stream()).is_closed():
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

    def __get_all_filters(self):
        filters = list()
        for layer in self.__layers:
            filters.extend(layer)
        return filters

    def __filter_streams_dict(self)->Mapping:
        '''
        Creates a dictionary with filters as keys that contains the list of input and output streams for that filter.
        '''
        filter_streams_dict = dict()
        for filter in self.__get_all_filters():
            filter_streams_dict[filter] = {'inputs' : [], 'outputs': []}
            for in_stream in filter.get_input():
                filter_streams_dict[filter]['inputs'].append(self.stream_dict.setdefault(in_stream, Stream()))
            for out_stream in filter.get_output():
                filter_streams_dict[filter]['outputs'].append(self.stream_dict.setdefault(out_stream, Stream()))
        return filter_streams_dict