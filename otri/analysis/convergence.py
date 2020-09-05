
import functools
from datetime import datetime, timedelta, timezone

from ..filtering.filter import Any, Filter, Mapping
from ..filtering.filter_net import EXEC_AND_PASS, FilterLayer, FilterNet
from ..filtering.filters.align_filter import AlignFilter
from ..filtering.filters.generic_filter import (GenericFilter,
                                                MultipleGenericFiler)
from ..filtering.filters.group_filter import TimeseriesGroupFilter
from ..filtering.filters.threshold_filter import ThresholdFilter
from ..utils import key_handler as kh
from ..utils import time_handler as th
from . import Analysis, Sequence, Stream


class RatioFilter(Filter):
    '''
    Calculates the average ratio between two prices streams every given time group.
    '''
    def __init__(self, inputs: Sequence[str], outputs: Sequence[str], time_group: timedelta = timedelta(seconds=3600), price_key: str = 'close'):
        '''
        Parameters:\n
            inputs : Sequence[str]
                Input stream names.\n
            outputs : Sequence[str]
                Output stream names.\n
            time_group : timedelta
                After how much time the calculation of the average ratio splits. eg. timedelta(day=1) the filter will return the average ratio for every day.\n
            price_key : str
                Key that contains the price value.\n
        '''
        super().__init__(
            inputs=inputs,
            outputs=outputs,
            input_count=2,
            output_count=2
        )
        self.__time_group = time_group
        self.__price_key = price_key

    def setup(self, inputs: Sequence[Stream], outputs: Sequence[Stream], state: Mapping[str, Any]):
        # Call superclass setup
        super().setup(inputs, outputs, state)
        self.__state = state
        self.__state['ratio'] = {}
        self.__atoms = [None, None]
        self.__ratio_sum = 0
        self.__counter = 0
        self.__interval_counter = 0
        self.__interval_atoms_counter = 0
        self.__next_interval = datetime(1, 1, 1, 1, 1, tzinfo=timezone.utc)

    def _on_data(self, data: Any, index: int):
        '''
        Checks if it received both atoms and calculates the ratio between them.
        It then uses the sum of the ratios to calculate the average ratio for every interval or length time_group.
        '''
        self.__atoms[index] = data
        # Update atoms counter (for input stream selection)
        self.__counter += 1
        if self.__atoms[0] is not None and self.__atoms[1] is not None:
            # Update interval atoms counter
            self.__interval_atoms_counter += 1
            # Atom ratio
            ratio = float(self.__atoms[0][self.__price_key])/float(self.__atoms[1][self.__price_key])
            # Rate sum
            self.__ratio_sum += ratio
            # Update average ratio
            self.__state['ratio'][th.datetime_to_str(self.__next_interval)] = self.__ratio_sum/self.__interval_atoms_counter
            # Remove atoms
            self.__atoms[0] = self.__atoms[1] = None

        # Check if it passed the ratio interval
        if th.str_to_datetime(data['datetime']) >= self.__next_interval:
            # Reset ratio
            self.__next_interval = th.str_to_datetime(data['datetime']) + self.__time_group
            self.__ratio_sum = 0
            self.__interval_counter += 1
            self.__interval_atoms_counter = 0
        self._push_data(data, index=index)

    def _input_check_order(self) -> Sequence[int]:
        '''
        Defines the order for the inputs to be checked.
        '''
        return [0] if self.__counter % 2 == 0 else [1]


def ratio_atom_func(avg_ratio: float, elements):
    # Two elements: atom from stream 1 and atom from stream 2
    new_atom = dict()
    new_atom['close'] = (float(elements[0]['close'])/float(elements[1]['close']) / avg_ratio) - 1
    return new_atom


class ConvergenceAnalysis(Analysis):
    '''
    Calculates the ratio between two time series in different periods and returns its value and its variance.
    '''

    def __init__(self, group_resolution: timedelta = timedelta(hours=4), ratio_interval: timedelta = timedelta(days=1)):
        '''
        Parameters:\n
            group_resolution : timedelta
                Resolution to group atoms to. Must be greater than atoms' resolution.\n
            ratio_interval : timedelta
                Interval of time where to calculate the average ratio. Must be greater than group_resolution\n
        '''
        self.__group_resolution = group_resolution
        self.__ratio_interval = ratio_interval

    def execute(self, input_streams: Sequence[Stream]):
        '''
        Starts convercence analyis.\n

        Parameters:\n
            in_streams : Stream
                Two time series streams to analyise, must contain same-interval atoms with 'close' key.
        '''
        # Prepare output_streams
        output_streams = [Stream(), Stream()]
        # Calculate ratios ever ratio_interval
        convergence_net = FilterNet(layers=[
            FilterLayer([
                # Tuple extractor
                # TODO: AVOID Tuple-extracting here
                GenericFilter(
                    inputs="s1",
                    outputs="atoms1",
                    operation=lambda element: element[1]
                ),
                GenericFilter(
                    inputs="s2",
                    outputs="atoms2",
                    operation=lambda element: element[1]
                )
            ], EXEC_AND_PASS),
            FilterLayer([
                # To Lowercase
                GenericFilter(
                    inputs="atoms1",
                    outputs="lower_s1",
                    operation=kh.lower_all_keys_deep
                ),
                GenericFilter(
                    inputs="atoms2",
                    outputs="lower_s2",
                    operation=kh.lower_all_keys_deep
                )
            ], EXEC_AND_PASS),
            FilterLayer([
                # Interpolation
                TimeseriesGroupFilter(
                    inputs="lower_s1",
                    outputs="grouped_s1",
                    target_resolution=self.__group_resolution
                ),
                TimeseriesGroupFilter(
                    inputs="lower_s2",
                    outputs="grouped_s2",
                    target_resolution=self.__group_resolution
                )
            ], EXEC_AND_PASS),
            FilterLayer([
                # Align datetime
                AlignFilter(
                    inputs=["grouped_s1", "grouped_s2"],
                    outputs=["align_s1", "align_s2"]
                )
            ], EXEC_AND_PASS),
            FilterLayer([
                # Rate calc
                RatioFilter(
                    inputs=["align_s1", "align_s2"],
                    outputs=["o1", "o2"],
                    time_group=self.__ratio_interval
                )
            ], EXEC_AND_PASS)
        ]).execute(source={"s1": input_streams[0], "s2": input_streams[1], "o1": output_streams[0], "o2": output_streams[1]})

        ratios = convergence_net.state("ratio", {})

        # Calculate average ratio
        average_ratio = 0
        for ratio in ratios.values():
            average_ratio += ratio
        if len(ratios) > 0:
            average_ratio /= len(ratios)

        # Calculate variance
        variance = 0
        for ratio in ratios.values():
            variance += (ratio - average_ratio) ** 2
        if len(ratios) > 0:
            variance /= len(ratios)

        # Calculate probability samples

        samples_net = FilterNet(layers=[
            FilterLayer([
                # Rate as atoms
                MultipleGenericFiler(
                    inputs=["s1", "s2"],
                    outputs="ratio",
                    operation=functools.partial(ratio_atom_func, average_ratio)
                )
            ], EXEC_AND_PASS),
            FilterLayer([
                # Sample density
                ThresholdFilter(
                    inputs="ratio",
                    outputs="o",
                    price_keys=['close'],
                    step=lambda i: round(i*0.01, ndigits=4)
                )
            ], EXEC_AND_PASS)]).execute(source={"s1": output_streams[0], "s2": output_streams[1]})

        samples_dict = samples_net.state(key='thresholds', default={})

        return {"ratios": ratios, "average_ratio": average_ratio, "variance": variance, "samples": samples_dict}
