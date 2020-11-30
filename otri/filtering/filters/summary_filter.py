from ..filter import Filter, Stream, Sequence, Mapping, Any
from typing import Callable, Collection
from ...utils import time_handler as th

DEFAULT_STATE_NAME = "summary"
RANGE_NAME = "range"
DATE_RANGE = "dateRange"
STRINGS_NAME = "strings"
COUNT_NAME = "count"

class SummaryFilter(Filter):
    '''
    Filter that calculates statistics on seen data.

    Inputs:
        A single queue of atoms/dicts.
    Ouputs:
        The same queue, nothing changed.
    '''

    def __init__(self, inputs: str, outputs: str, state_name = DEFAULT_STATE_NAME):
        '''
        Parameters:
            inputs : str
                Input stream name.
            outputs : str
                Output stream name.
        '''
        super().__init__(
            inputs=[inputs],
            outputs=[outputs],
            input_count=1,
            output_count=1
        )
        self.__state_name = state_name

    def setup(self, inputs: Sequence[Stream], outputs: Sequence[Stream], state: Mapping[str, Any]):
        '''
        Used to save references to streams and reset variables.
        Called once before the start of the execution in FilterNet.

        Parameters:
            inputs, outputs : Sequence[Stream]
                Ordered sequence containing the required input/output streams gained from the FilterNet.
            state : Mapping[str, Any]
                Dictionary containing states to output.
        '''
        # Call superclass setup
        super().setup(inputs, outputs, state)
        # Save the state instance
        self.__state = state
        self.__state[self.__state_name] = dict()
    
    def _on_data(self, data, index):
        '''
        Calculates statistics then passes data to the output.
        '''
        for key, value in data.items():
            if value is None:
                continue
            key_state_dict = self.__state[self.__state_name].get(key, dict())
            if type(value) == int or type(value) == float or value.replace('.','',1).isnumeric(): # Numeric values
                f_value = float(value)
                cur_range = key_state_dict.get(RANGE_NAME, [float("inf"),float("-inf")])
                if f_value < cur_range[0]:
                    cur_range[0] = f_value
                if f_value > cur_range[1]:
                    cur_range[1] = f_value
                key_state_dict[RANGE_NAME] = cur_range
            elif th.is_datetime(value):
                cur_range = key_state_dict.get(DATE_RANGE, ["9999999999999999999999999","0"])
                if value < cur_range[0]:
                    cur_range[0] = value
                if value > cur_range[1]:
                    cur_range[1] = value
                key_state_dict[DATE_RANGE] = cur_range
            else:
                cur_set = key_state_dict.get(STRINGS_NAME, set())
                cur_set.add(value)
                key_state_dict[STRINGS_NAME] = cur_set

            # Occurrencies count
            key_state_dict[COUNT_NAME]  = key_state_dict.get(COUNT_NAME, 0) + 1
            self.__state[self.__state_name][key] = key_state_dict
                
        self._push_data(data)
