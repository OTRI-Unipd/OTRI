
__author__ = "Luca Crema <lc.crema@hotmail.com>"

from ..filter import Filter, Sequence, Any, Mapping, Stream
import math


class ThresholdFilter(Filter):
    '''
    Calculates the number of times that a stream goes over or under 0 after changing value sign.
    For every time the stream goes over (or under) it assigns a +1 on a level value when it first reaches it, then locks the assignment of other +1 on that level until the value
    changes sign or reaches its complementary.\n
    This calculation is made for every given key.
    '''

    def __init__(self,  inputs: str, outputs: str, price_keys: Sequence[str] = ['open', 'low', 'high', 'close'], state_name: str = 'thresholds', step: callable = lambda i: round(i*0.0001, ndigits=5)):
        '''
        Parameters:\n
            inputs : str
                Input stream name.\n
            outputs : str
                Output stream name.\n
            price_keys : Sequence[str]
                Keys that contains price values.\n
            state_name : str
                Name of the state where to save values.\n
            step : callable
                Function that takes a number and returns the value associated to that level.\n
        '''
        super().__init__(
            inputs=[inputs],
            outputs=[outputs],
            input_count=1,
            output_count=1
        )
        self.__price_keys = price_keys
        self.__state_name = state_name
        self.__step = step

    def setup(self, inputs: Sequence[Stream], outputs: Sequence[Stream], state: Mapping[str, Any]):
        # Call superclass setup
        super().setup(inputs, outputs, state)
        self.__state = state
        self.__state[self.__state_name] = dict()
        self.__levels_lock = dict()
        for key in self.__price_keys:
            self.__state[self.__state_name][key] = dict()
            self.__levels_lock[key] = dict()  # {'close': {'0.01': False}}

    def _on_data(self, data: Any, index: int):
        '''
        Counts how many times the values gets over or under a number of levels.
        '''
        for key in self.__price_keys:
            value = data[key]
            # Reset locks if it changed sign
            self.__reset_all_locks(key, value)
            # Iter through every step from 0 to value and update level values if not locked
            i = 0
            abs_step = self.__step(i)
            while abs_step <= abs(value):
                signed_step = str(abs_step * math.copysign(1, value))  # str because it differs from -0.0 and +0.0, while float -0.0 is just like +0.0 in dict keys
                if self.__levels_lock[key].setdefault(signed_step, False) is False:
                    self.__levels_lock[key][signed_step] = True
                    self.__state[self.__state_name][key].setdefault(signed_step, 0)
                    self.__state[self.__state_name][key][signed_step] += 1
                    print("+1 on {}".format(signed_step))
                i += 1
                abs_step = self.__step(i)

        self._push_data(data, index=index)

    def __reset_all_locks(self, key: str, value: float):
        '''
        Removes the lock from all levels if the sign has changed. Called when the value changes sign.
        '''
        sign = value > 0
        # Skip first iteration
        if self.__levels_lock[key].get('0.0', None) is None:
            return
        if sign is not self.__levels_lock[key]['0.0']:
            # If the sign has changed reset all locks to false
            for value_key in self.__levels_lock[key].keys():
                self.__levels_lock[key][value_key] = False
