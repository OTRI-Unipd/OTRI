from otri.utils import key_handler as kh
from otri.filtering.stream import Stream

from typing import Set, Sequence, Any, Callable
import time
from statistics import mean
from . import Analysis

from otri.filtering.filter_net import EXEC_AND_PASS, FilterNet
from otri.filtering.filter_layer import FilterLayer
from otri.filtering.filters.generic_filter import GenericFilter
from otri.filtering.filters.nuplicator_filter import NUplicatorFilter

from otri.validation.validators.cluster_validator import ClusterValidator


class ClusterAnalysis(Analysis):

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
        stream_per_key = [key for key in self.keys]
        output_per_key = [key + "_out" for key in self.keys]

        start_time = time.time()
        analysis_net = FilterNet([
            FilterLayer([
                # Tuple extractor
                GenericFilter(
                    inputs="db_tuples",
                    outputs="db_atoms",
                    operation=lambda element: element[0]
                )
            ], EXEC_AND_PASS),
            FilterLayer([
                # To Lowercase
                GenericFilter(
                    inputs="db_atoms",
                    outputs="lower_atoms",
                    operation=lambda atom: kh.lower_all_keys_deep(atom)
                )
            ], EXEC_AND_PASS),
            FilterLayer([
                # N-Uplicate
                NUplicatorFilter(
                    inputs="lower_atoms",
                    outputs=stream_per_key,
                    deep_copy=True
                )
            ], EXEC_AND_PASS),
            FilterLayer([
                # Check For clusters
                ClusterValidator(
                    inputs=each_stream,
                    outputs=each_output,
                    key=key,
                    limit=1
                ) for key, each_stream, each_output
                in zip(self.keys, stream_per_key, output_per_key)
            ], EXEC_AND_PASS)
        ]).execute({"db_tuples": in_streams[0]}, on_data_output=self.on_output)

        elapsed_time = time.time() - start_time
        outputs = [list(analysis_net.streams()[out]) for out in output_per_key]

        total = len(outputs[0])

        state = analysis_net.state_dict
        print(state)
        flagged = {k: sum(v) for k, v in state.items() if v}
        avg = {k: mean(v) for k, v in state.items() if v}

        return {"mean": avg, "flagged": flagged}, 0, total, elapsed_time
