from otri.utils import key_handler as kh
from otri.filtering.stream import Stream

from typing import Set, Sequence, Any, Callable
import time
from . import Analysis

from otri.filtering.filter_net import BACK_IF_NO_OUTPUT, EXEC_AND_PASS, FilterNet
from otri.filtering.filter_layer import FilterLayer
from otri.filtering.filters.generic_filter import GenericFilter

from otri.validation import MonoValidator
from otri.validation.exceptions import RangeError
from otri.validation.valchecks import check_positive


class FindNegativesAnalysis(Analysis):

    def __init__(self, keys: Set[str], on_output: Callable):
        '''
        Parameters:
            keys : Set[str]
                The keys that must not be null.
            on_output : Callable
                A function requiring no parameters to call every time the network outputs something.
        '''
        self.keys = keys
        self.on_output = on_output

    def execute(self, in_streams: Sequence[Stream]) -> Any:
        '''
        Starts data analyis.\n

        Parameters:
            in_streams : Stream
                Required Streams: single Stream from the database.
        Returns:
            result: None.
            flagged: How many atoms were flagged.
            total: How many atoms got through.
            elapsed_time: How much the computation took, in seconds.
        '''
        start_time = time.time()
        analysis_net = FilterNet([
            FilterLayer([
                # Tuple extractor
                GenericFilter(
                    inputs="db_tuples",
                    outputs="db_atoms",
                    operation=lambda element: element[1]
                )
            ], EXEC_AND_PASS),
            FilterLayer([
                # To Lowercase
                GenericFilter(
                    inputs="db_atoms",
                    outputs="lower_atoms",
                    operation=lambda atom: kh.lower_all_keys_deep(atom)
                )
            ], BACK_IF_NO_OUTPUT),
            FilterLayer([
                # Check Non-null
                MonoValidator(
                    inputs="lower_atoms",
                    outputs="output",
                    check=lambda atom: check_positive(
                        atom, self.keys
                    )
                )
            ], EXEC_AND_PASS)
        ]).execute({"db_tuples": in_streams[0]}, on_data_output=self.on_output)

        elapsed_time = time.time() - start_time
        output = list(analysis_net.streams()["output"])

        total = len(output)
        flagged = len(list(filter(lambda x: RangeError.KEY in x.keys(), output)))

        return None, flagged, total, elapsed_time
