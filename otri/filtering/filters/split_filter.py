from ..filter import Filter
from typing import Sequence, Any, Collection, Set
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

    def __init__(self, source_stream: Stream, key: Any, ranges: Sequence, ignore_none: bool = True, side: str = 'left'):
        '''
        Parameters:
            source_stream : Stream
                A single Stream that must be split.
            key : Any
                The key on which to split.
            ranges : Sequence
                The N ranges (r1, r2, ..., rn) for which to split. Will be used as sorted(ranges).
                The ouput streams will be N+1 or N+2 : less than r1, more than rn, the n-1 in-betweens,
                and optionally one for the atoms that do not have the key, if enabled.
            ignore_none : bool = True
                Whether to ignore the atoms that don't have the key or not. Default is True, the atoms will
                be ignored, deleted, and the Filter will attempt to fetch another one.
                If set to False, an output Stream is dedicated to such atoms.
            side : str = 'left'
                Same as `numpy.searchsorted`. 'left' makes an interval from v1 to v2 as ]v1,v2], while 'right'
                makes an interval as [v1,v2[. 
        '''
        n = len(ranges)
        super().__init__(
            input_streams=[source_stream],
            input_streams_count=1,
            output_streams_count=n + 1 if ignore_none else n + 2
        )
        self.__key = key
        self.__side = side
        self.__ranges = ranges
        self.__ignore_none = ignore_none
        self.__source_iter = source_stream.__iter__()
        self.__none_stream = self.get_output_stream(n+1)

    def execute(self):
        '''
        Attempts to fetch a single atom from the input stream. Puts it into the appropriate output stream.
        If the atoms that do not have the key should be discarded, it also attempts to fetch another one
        immediately.
        '''
        # Assumes that all outputs must be closed at once.
        if self.get_output_stream(0).is_closed():
            return
        if self.__source_iter.has_next():
            item = self.__source_iter.__next__()
            if self.__key in item.keys():
                # Find the appropriate Stream for the item.
                self.get_output_stream(numpy.searchsorted(
                    self.__ranges, item[self.__key], self.__side
                )).append(item)
            else:
                # Ignoring the item that does not have the key.
                if self.__ignore_none:
                    self.execute()
                # Putting the item in the dedicated Stream.
                else:
                    self.__none_stream.append(item)
        elif self.get_input_stream(0).is_closed():
            # Closed input -> Close outputs
            for output in self.get_output_streams():
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

    def __init__(self, source_stream: Stream, key: Any, cases: Set, ignore_none: bool = True):
        '''
        Parameters:
            source_stream : Stream
                A single Stream that must be split.
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
        n = len(ranges)
        super().__init__(
            input_streams=[source_stream],
            input_streams_count=1,
            output_streams_count=n + 1 if ignore_none else n + 2
        )
        self.__key = key
        self.__cases = cases
        self.__ignore_none = ignore_none
        self.__source_iter = source_stream.__iter__()
        self.__none_stream = self.get_output_stream(n+1)
        self.__cases_outputs = {
            case: self.get_output_stream(i)
            for i, case in enumerate(cases)
        }

    def get_case_output_stream(self, case: Any):
        '''
        Parameters:
            case : Any
                One of the cases for this filter
        Returns: Stream
            The output Stream relative to the 
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
        if self.get_output_stream(0).is_closed():
            return
        if self.__source_iter.has_next():
            item = self.__source_iter.__next__()
            if key in item.keys():
                # Put the atom in the appropriate output stream.
                if key in self.__cases_outputs.keys():
                    self.__cases_outputs[item[key]].append(item)
                else:
                    self.get_output_stream(len(self.__cases)).append(item)
            else:
                # Ignoring the item that does not have the key.
                if self.__ignore_none:
                    self.execute()
                # Putting the item in the dedicated Stream.
                else:
                    self.__none_stream.append(item)
        elif self.get_input_stream(0).is_closed():
            # Closed input -> Close outputs
            for output in self.get_output_streams():
                output.close()
