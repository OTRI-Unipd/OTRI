from ..filter import Filter, Stream, Sequence, Any, Mapping
from typing import Sequence, Mapping, Collection
from numbers import Number


class StatisticsFilter(Filter):
    '''
    Pops a single piece of data, reads the fields in the `keys` init parameter, updates the state
    and outputs the data unmodified.

    Input:
        Single stream.
    Output:
        Single stream.
    '''

    def __init__(self, input: str, output: str, keys: Collection[str]):
        '''
        Parameters:
            input : str
                Input stream name.
            output : str
                Output stream name.
            keys : Sequence
                The keys for which to compute the stats.
        '''
        super().__init__(
            input=[input],
            output=[output],
            input_count=1,
            output_count=1
        )
        self.__keys = keys
        # Dict like : {callable : status_name}
        self.__ops = dict()


    def execute(self, inputs: Sequence[Stream], outputs: Sequence[Stream], status: Mapping[str, Any]):
        '''
        Pops a single piece of data, reads the fields in the `keys` init parameter, updates the state
        and outputs the data unmodified.

        Parameters:
            inputs, outputs : Sequence[Stream]
                Ordered sequence containing the required input/output streams gained from the FilterList.
        '''
        if outputs[0].is_closed():
            return
        if iter(inputs[0]).has_next():
            atom = next(iter(inputs[0]))
            for op in self.__ops.keys():
                op(atom, status)
            outputs[0].append(atom)
        # Check that we didn't just pop the last item
        elif inputs[0].is_closed():
            outputs[0].close()

    def calc_avg(self, status_name: str):
        '''
        Enable calculating the Average, by enabling both the sum and the count.
        Redundant enabling is a no-op.

        Parameters:
            status_name : str
                Naming for the key that will contain this status value.
        '''
        self.calc_sum("avg_sum")
        self.calc_count("avg_count")
        if self.__avg not in self.__ops.keys():
            self.__ops[self.__avg] = status_name
        return self

    def calc_sum(self, status_name: str):
        '''
        Enable calculating the sum.
        Redundant enabling is a no-op.

        Parameters:
            status_name : str
                Naming for the key that will contain this status value.
        '''
        self.__ops[self.__sum] = status_name
        return self

    def calc_count(self, status_name: str):
        '''
        Enable counting.
        Redundant enabling is a no-op.

        Parameters:
            status_name : str
                Naming for the key that will contain this status value.
        '''
        self.__ops[self.__count] = status_name
        return self

    def calc_max(self, status_name: str):
        '''
        Enable finding the max.
        Redundant enabling is a no-op.

        Parameters:
            status_name : str
                Naming for the key that will contain this status value.
        '''
        self.__ops[self.__max] = status_name
        return self

    def calc_min(self, status_name: str):
        '''
        Enable finding the min.
        Redundant enabling is a no-op.

        Parameters:
            status_name : str
                Naming for the key that will contain this status value.
        '''
        self.__ops[self.__min] = status_name
        return self

    def __sum(self, atom: Mapping, status: Mapping):
        '''
        Update the sum state for the given keys.
        '''
        for k in self.__keys:
            if k in atom.keys():
                status[self.__ops[self.__sum]][k] = status.setdefault(
                    self.__ops[self.__sum], dict()).setdefault(k,0) + atom[k]

    def __count(self, atom: Mapping, status: Mapping):
        '''
        Update the count state for the given keys.
        '''
        for k in self.__keys:
            if k in atom.keys():
                status[self.__ops[self.__count]][k] = status.setdefault(self.__ops[self.__count],dict())\
                                                            .setdefault(k, 0) + 1

    def __max(self, atom: Mapping, status: Mapping):
        '''
        Update the max state for the given keys.
        '''
        for k in self.__keys:
            if k in atom.keys():
                val = status.setdefault(self.__ops[self.__max],dict()).setdefault(k, '-inf')
                status[self.__ops[self.__max]][k] = max(val, atom[k])

    def __min(self, atom: Mapping, status: Mapping):
        '''
        Update the min state for the given keys.
        '''
        for k in self.__keys:
            if k in atom.keys():
                val = status.setdefault(self.__ops[self.__min],dict()).setdefault(k, 'inf')
                status[self.__ops[self.__min]][k] = min(val, atom[k])

    def __avg(self, atom : Mapping, status : Mapping):
        '''
        Update the avg state for the given keys.
        '''
        for k in self.__keys:
            if k in atom.keys():
                if(status.setdefault(self.__ops[self.__count],dict()).setdefault(k,0) != 0):
                    status.setdefault(self.__ops[self.__avg], dict())[k] = status.setdefault(self.__ops[self.__sum],dict()).setdefault(k,0) / status[self.__ops[self.__count]][k]
