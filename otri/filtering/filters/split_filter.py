from ..filter import Filter, Stream, Sequence, Mapping, Any
from typing import Set
import numpy


class SplitFilter(Filter):
    '''
    Splits a Stream based on the value range of a numerical field in the atoms.
    Inputs: a single Stream.
    Outputs: Multiple Streams, as many as the possible value ranges are, plus one for the
    atoms that do not have the given key, if enabled. The indexes will be treated as:
    0 for the left-most interval (less than r1),
    1 for the second interval (r1 to r2),
    and so on. If atoms that do not have the given key are not to be ignored, the stream
    will be the last one (of index n+1).
    '''

    def __init__(self, input: str, output : Sequence[str], key: Any, ranges: Sequence, none_keys_output: str = None, side: str = 'left'):
        '''
        Parameters:
            input : str
                A single stream name.
            output : str
                The output streams names. Must have the same length as the number of ranges + 1
            key : Any
                The key on which to split.
            ranges : Sequence
                The N ranges (r1, r2, ..., rn with i>j ri>rj) for which to split. Will be used as sorted(ranges).
                The ouput streams will be N+1 or N+2 : less than r1, more than rn, the n-1 in-betweens,
                and optionally one for the atoms that do not have the key, if enabled.
            ignored_output : str = None
                If a name is given atoms that don't have the key will be placed here.
                If None is given the atoms will be ignored, deleted, and the Filter will attempt to fetch another one.
            side : str = 'left'
                Same as `numpy.searchsorted`. 'left' makes an interval from v1 to v2 as ]v1,v2], while 'right'
                makes an interval as [v1,v2[. 
        '''
        n = len(ranges)
        if none_keys_output != None:
            output.append(none_keys_output)
            self.__ignore_none = False
        else:
            self.__ignore_none = True

        super().__init__(
            input=[input],
            output=output,
            input_count=1,
            output_count=n + 1 if self.__ignore_none else n + 2
        )
        self.__key = key
        self.__side = side
        self.__ranges = ranges

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
        self.__outputs = outputs
        if not self.__ignore_none:
            self.__none_output = outputs[len(outputs) - 1]

    def execute(self):
        '''
        Attempts to fetch a single atom from the input stream. Puts it into the appropriate output stream.
        If the atoms that do not have the key should be discarded, it also attempts to fetch another one
        immediately.
        '''
        # Assumes that all outputs must be closed at once.
        if self.__outputs[0].is_closed():
            return
        if self.__input_iter.has_next():
            item = next(self.__input_iter)
            if self.__key in item.keys():
                # Find the appropriate Stream for the item.
                self.__outputs(numpy.searchsorted(
                    self.__ranges, item[self.__key], self.__side
                )).append(item)
            else:
                # Ignoring the item that does not have the key.
                if not self.__ignore_none:
                    # Append void atom on last output
                   self.__none_output.append(item)
        elif self.__input.is_closed():
            # Closed input -> Close outputs
            for output in self.__outputs:
                output.close()


class SwitchFilter(Filter):
    '''
    Splits a Stream based on the value of a field in the atoms.
    Inputs: a single Stream.
    Outputs: Multiple Streams, as many as the requested cases (N), plus one for the default case,
    and one for the atoms that do not have the requested key, if enabled.
    Output streams of index 0 to N-1 will be the cases, N will be the default, N+1 will be
    for atoms that do not have the key (if enabled). N and N+1 are guaranteed, but the case
    ordering is not.
    '''

    def __init__(self, input: str, cases_output : Sequence[str], default_output : str, key: Any, cases: Set, none_keys_output: str = None):
        '''
        Parameters:
            input : str
                A single stream name.
            output : str
                The output streams names. Must have the same length as the number of ranges + 1
            key : Any
                The key on which to split.
            cases : Set
                The N values for which to split. Must be different values.
                The ouput streams will be N+1 or N+2 : the N cases, the default, and optionally one for
                the atoms that do not have the key, if enabled. Case ordering isn't guaranteed.
            ignore_none: bool = True
                Whether to ignore the atoms that don't have the key or not. Default is True, the atoms will
                be ignored, deleted, and the Filter will attempt to fetch another one.
                If set to False, an output Stream is dedicated to such atoms.
        '''
        n = len(cases)
        output = cases_output
        output.append(default_output)
        if none_keys_output != None:
            cases_output.append(none_keys_output)
            self.__ignore_none = False
        else:
            self.__ignore_none = True
        super().__init__(
            input=[input],
            output=output,
            input_count=1,
            output_count=n + 1 if self.__ignore_none else n + 2
        )
        self.__key = key
        self.__cases = cases

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
        self.__outputs = outputs
        if not self.__ignore_none:
            self.__default_output = outputs[len(outputs) - 2]
            self.__none_output = outputs[len(outputs) - 1]
        else:
            self.__default_output = outputs[len(outputs) - 1]
        self.__cases_outputs = {
            case: outputs[i]
            for i, case in enumerate(self.__cases)
        }

    def __get_case_output_stream(self, case: Any):
        '''
        Parameters:
            case : Any
                One of the cases for this filter
        Returns: Stream
            The output Stream relative to the case
        Raises:
            KeyError : if the case was not in the cases provided as init parameter.
        '''
        return self.__cases_outputs[case]

    def execute(self):
        '''
        Attempts to fetch a single atom from the input stream. Puts it into the appropriate output stream.
        If the atoms that do not have the key should be discarded, it also attempts to fetch another one
        immediately.
        '''
        # Assumes that all outputs must be closed at once.
        key = self.__key
        if self.__outputs[0].is_closed():
            return
        if self.__input_iter.has_next():
            item = next(self.__source_iter)
            if key in item.keys():
                # Put the atom in the appropriate output stream.
                if item[key] in self.__cases:
                    self.__cases_outputs[item[key]].append(item)
                else:
                    self.__default_output.append(item)
            else:
                if not self.__ignore_none:
                    # Putting the item in the dedicated Stream.
                    self.__none_output.append(item)
        elif self.__input.is_closed():
            # Closed input -> Close outputs
            for output in self.__outputs:
                output.close()
