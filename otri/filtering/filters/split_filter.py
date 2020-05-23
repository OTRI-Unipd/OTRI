from ..filter import Filter
from typing import Sequence, Any, Collection


class SplitFilter(Filter):
    '''
    Splits a Stream based on the value range of a numerical field in the atoms.
    Inputs: a single Stream.
    Outputs: Multiple Streams, as many as the possible value ranges are, plus one for the
    atoms that do not have the given key, if enabled.
    '''

    def __init__(self, source_stream: Stream, key: Any, ranges: Sequence, ignore_none: bool = True):
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
            ignore_none: bool = True
                Whether to ignore the atoms that don't have the key or not. Default is True, the atoms will
                be ignored, deleted, and the Filter will attempt to fetch another one.
                If set to False, an output Stream is dedicated to such atoms.
        '''
        super().__init__(
            input_streams=[source_stream],
            input_streams_count=1,
            output_streams_count=1  # TODO aaaaaaaaaaaaaaaaaaaaaaa
        )

    def execute(self):
        '''
        '''


class SwitchFilter(Filter):
    '''
    Splits a Stream based on the value of a field in the atoms.
    Inputs: a single Stream.
    Outputs: Multiple Streams, as many as the requested cases, plus one for the default case,
    and one for the atoms that do not have the requested key, if enabled.
    '''

    def __init__(self, source_stream: Stream, key: Any, cases: Collection, ignore_none: bool = True):
        '''
        Parameters:
            source_stream : Stream
                A single Stream that must be split.
            key : Any
                The key on which to split.
            cases : Collection
                The N values for which to split.
                The ouput streams will be N+1 or N+2 : the N cases, the default, and optionally one for
                the atoms that do not have the key, if enabled.
            ignore_none: bool = True
                Whether to ignore the atoms that don't have the key or not. Default is True, the atoms will
                be ignored, deleted, and the Filter will attempt to fetch another one.
                If set to False, an output Stream is dedicated to such atoms.
        '''
        super().__init__(
            input_streams=[source_stream],
            input_streams_count=1,
            output_streams_count=1  # TODO aaaaaaaaaaaaaaaaaaaaaaa
        )

    def execute(self):
        '''
        '''
