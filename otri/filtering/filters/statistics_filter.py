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
            keys : Collection[str]
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

    def setup(self, inputs : Sequence[Stream], outputs : Sequence[Stream], status: Mapping[str, Any]):
        '''
        Used to save references to streams and reset variables.
        Called once before the start of the execution in FilterList.
        
        Parameters:
            inputs, outputs : Sequence[Stream]
                Ordered sequence containing the required input/output streams gained from the FilterList.
            status : Mapping[str, Any]
                Dictionary containing statuses to output.
        '''
        self.__input = inputs[0]
        self.__input_iter = iter(inputs[0])
        self.__output = outputs[0]
        self.__status = status
        for stat_name in self.__ops.values():
            if status.get(stat_name, None) != None:
                raise(ValueError("status '{}' uses duplicate name".format(stat_name)))
            status[stat_name] = dict()

    def execute(self):
        '''
        Pops a single piece of data, reads the fields in the `keys` init parameter, updates the state
        and outputs the data unmodified.
        '''
        if self.__output.is_closed():
            return
        if self.__input_iter.has_next():
            atom = next(self.__input_iter)
            for op, stat_name in self.__ops.items():
                op(atom, stat_name)
            self.__output.append(atom)
        # Check that we didn't just pop the last item
        elif self.__input.is_closed():
            self.__output.close()

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

    def __sum(self, atom: Mapping, stat_name : str):
        '''
        Update the sum state for the given keys.
        '''
        for k in self.__keys:
            if k in atom.keys():
                self.__status[stat_name][k] = self.__status[stat_name].setdefault(k,0) + atom[k]

    def __count(self, atom: Mapping, stat_name : str):
        '''
        Update the count state for the given keys.
        '''
        for k in self.__keys:
            if k in atom.keys():
                self.__status[stat_name][k] = self.__status[stat_name].setdefault(k, 0) + 1

    def __max(self, atom: Mapping, stat_name : str):
        '''
        Update the max state for the given keys.
        '''
        for k in self.__keys:
            if k in atom.keys():
                old_val = self.__status[stat_name].setdefault(k, float('-inf'))
                self.__status[stat_name][k] = max(old_val, atom[k])

    def __min(self, atom: Mapping, stat_name : str):
        '''
        Update the min state for the given keys.
        '''
        for k in self.__keys:
            if k in atom.keys():
                old_val = self.__status[stat_name].setdefault(k, float('inf'))
                self.__status[stat_name][k] = min(old_val, atom[k])

    def __avg(self, atom : Mapping, stat_name : str):
        '''
        Update the avg state for the given keys.
        '''
        for k in self.__keys:
            if k in atom.keys():
                if(self.__status[self.__ops[self.__count]].setdefault(k,0) != 0):
                    avg = self.__status[self.__ops[self.__sum]].setdefault(k,0) / self.__status[self.__ops[self.__count]][k]
                    self.__status[stat_name][k] = avg
