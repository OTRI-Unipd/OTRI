from ..filter import Filter, Queue, Sequence, Any, Mapping
from typing import Sequence, Mapping, Collection
from numbers import Number


class StatisticsFilter(Filter):
    '''
    Reads the fields in the `keys` init parameter, updates the state and outputs the data unmodified.

    Inputs:
        Single queue.
    Outputs:
        Single queue.
    '''

    def __init__(self, inputs: str, outputs: str, keys: Collection[str]):
        '''
        Parameters:
            inputs : str
                Input queue name.
            outputs : str
                Output queue name.
            keys : Collection[str]
                The keys for which to compute the stats.
        '''
        super().__init__(
            inputs=[inputs],
            outputs=[outputs],
            input_count=1,
            output_count=1
        )
        self.__keys = keys
        # Dict like : {callable : state_name}
        self.__ops = dict()

    def setup(self, inputs: Sequence[Queue], outputs: Sequence[Queue], state: Mapping[str, Any]):
        '''
        Used to save references to queues and reset variables.
        Called once before the start of the execution in FilterNet.

        Parameters:
            inputs, outputs : Sequence[Queue]
                Ordered sequence containing the required input/output queues gained from the FilterNet.
            state : Mapping[str, Any]
                Dictionary containing states to output.
        '''
        # Call superclass setup
        super().setup(inputs, outputs, state)
        # Save the state instance
        self.__state = state
        # Check if state name is already used, otherwise init a dict
        for stat_name in self.__ops.values():
            if state.get(stat_name, None) != None:
                raise(ValueError("state '{}' uses duplicate name".format(stat_name)))
            state[stat_name] = dict()

    def _on_data(self, data, index):
        '''
        Pops a single piece of data, reads the fields in the `keys` init parameter, updates the state
        and outputs the data unmodified.
        '''
        # Update all of the operations
        for op, stat_name in self.__ops.items():
            op(data, stat_name)
        # Output data unmodified
        self._push_data(data)

    def calc_avg(self, state_name: str):
        '''
        Enable calculating the Average, by enabling both the sum and the count.
        Redundant enabling is a no-op.

        Parameters:
            state_name : str
                Naming for the key that will contain this state value.
        '''
        self.calc_sum("avg_sum")
        self.calc_count("avg_count")
        if self.__avg not in self.__ops.keys():
            self.__ops[self.__avg] = state_name
        return self

    def calc_sum(self, state_name: str):
        '''
        Enable calculating the sum.
        Redundant enabling is a no-op.

        Parameters:
            state_name : str
                Naming for the key that will contain this state value.
        '''
        self.__ops[self.__sum] = state_name
        return self

    def calc_count(self, state_name: str):
        '''
        Enable counting.
        Redundant enabling is a no-op.

        Parameters:
            state_name : str
                Naming for the key that will contain this state value.
        '''
        self.__ops[self.__count] = state_name
        return self

    def calc_max(self, state_name: str):
        '''
        Enable finding the max.
        Redundant enabling is a no-op.

        Parameters:
            state_name : str
                Naming for the key that will contain this state value.
        '''
        self.__ops[self.__max] = state_name
        return self

    def calc_min(self, state_name: str):
        '''
        Enable finding the min.
        Redundant enabling is a no-op.

        Parameters:
            state_name : str
                Naming for the key that will contain this state value.
        '''
        self.__ops[self.__min] = state_name
        return self

    def __sum(self, atom: Mapping, stat_name: str):
        '''
        Update the sum state for the given keys.
        '''
        for k in self.__keys:
            if k in atom.keys():
                self.__state[stat_name][k] = self.__state[stat_name].setdefault(k, 0) + atom[k]

    def __count(self, atom: Mapping, stat_name: str):
        '''
        Update the count state for the given keys.
        '''
        for k in self.__keys:
            if k in atom.keys():
                self.__state[stat_name][k] = self.__state[stat_name].setdefault(k, 0) + 1

    def __max(self, atom: Mapping, stat_name: str):
        '''
        Update the max state for the given keys.
        '''
        for k in self.__keys:
            if k in atom.keys():
                old_val = self.__state[stat_name].setdefault(k, float('-inf'))
                self.__state[stat_name][k] = max(old_val, atom[k])

    def __min(self, atom: Mapping, stat_name: str):
        '''
        Update the min state for the given keys.
        '''
        for k in self.__keys:
            if k in atom.keys():
                old_val = self.__state[stat_name].setdefault(k, float('inf'))
                self.__state[stat_name][k] = min(old_val, atom[k])

    def __avg(self, atom: Mapping, stat_name: str):
        '''
        Update the avg state for the given keys.
        '''
        for k in self.__keys:
            if k in atom.keys():
                if(self.__state[self.__ops[self.__count]].setdefault(k, 0) != 0):
                    avg = self.__state[self.__ops[self.__sum]].setdefault(
                        k, 0) / self.__state[self.__ops[self.__count]][k]
                    self.__state[stat_name][k] = avg
