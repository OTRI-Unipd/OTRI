
from datetime import datetime, timedelta, timezone

import numpy as np

from ..filtering.filter import Any, Filter, Mapping
from ..filtering.filter_net import EXEC_AND_PASS, FilterLayer, FilterNet
from ..filtering.filters.align_filter import AlignFilter
from ..filtering.filters.generic_filter import GenericFilter, MultipleGenericFiler
from ..filtering.filters.group_filter import GroupFilter
from ..filtering.filters.threshold_filter import ThresholdFilter
from ..utils import key_handler as kh
from ..utils import time_handler as th
from . import Analysis, Sequence, Stream
import functools


class RateCalcFilter(Filter):

    def __init__(self, inputs: Sequence[str], outputs: Sequence[str], split_every: timedelta = timedelta(seconds=3600), price_key: str = 'close'):
        '''
        Parameters:\n
            inputs : Sequence[str]
                Input stream names.\n
            outputs : Sequence[str]
                Output stream names.\n
            split_every : timedelta
                Timespan to calculate average rate.\n
            price_key : str
                Key that contains the price value.\n
        '''
        super().__init__(
            inputs=inputs,
            outputs=outputs,
            input_count=2,
            output_count=2
        )
        self.__split_every = split_every
        self.__price_key = price_key

    def setup(self, inputs: Sequence[Stream], outputs: Sequence[Stream], state: Mapping[str, Any]):
        # Call superclass setup
        super().setup(inputs, outputs, state)
        self.__state = state
        self.__state['rate'] = {}
        self.__atoms = [None, None]
        self.__rate_sum = 0
        self.__counter = 0
        self.__interval_counter = 0
        self.__interval_atoms_counter = 0
        self.__next_interval = datetime(1, 1, 1, 1, 1, tzinfo=timezone.utc)

    def _on_data(self, data: Any, index: int):
        '''
        Checks if it received both atoms and calculates the rate between them.
        It then uses the sum of the rates to calculate the average rate for every interval or length split_every.
        '''
        self.__atoms[index] = data
        # Update atoms counter (for input stream selection)
        self.__counter += 1
        if self.__atoms[0] is not None and self.__atoms[1] is not None:
            # Update interval atoms counter
            self.__interval_atoms_counter += 1
            # Atom rate
            rate = self.__atoms[0][self.__price_key]/self.__atoms[1][self.__price_key]
            # Rate sum
            self.__rate_sum += rate
            # Update average rate
            self.__state['rate'][th.datetime_to_str(self.__next_interval)] = self.__rate_sum/self.__interval_atoms_counter
            # Remove atoms
            self.__atoms[0] = self.__atoms[1] = None

        # Check if it passed the rate interval
        if th.str_to_datetime(data['datetime']) >= self.__next_interval:
            # Reset rate
            self.__next_interval = th.str_to_datetime(data['datetime']) + self.__split_every
            self.__rate_sum = 0
            self.__interval_counter += 1
            self.__interval_atoms_counter = 0
        self._push_data(data, index=index)

    def _input_check_order(self) -> Sequence[int]:
        '''
        Defines the order for the inputs to be checked.
        '''
        return [0] if self.__counter % 2 == 0 else [1]


def rate_atom_func(avg_rate: float, elements):
    # Two elements: atom from stream 1 and atom from stream 2
    new_atom = dict()
    new_atom['close'] = (float(elements[0]['close'])/float(elements[1]['close']) / avg_rate) - 1
    return new_atom


class ConvergenceAnalysis(Analysis):
    '''
    Calculates the ratio between two time series in different periods and returns its value and its variance.
    '''

    def __init__(self, group_resolution: timedelta = timedelta(hours=4), rate_interval: timedelta = timedelta(days=1)):
        '''
        Parameters:\n
            group_resolution : timedelta
                Resolution to group atoms to. Must be greater than atoms' resolution.\n
            rate_interval : timedelta
                Interval of time where to calculate the average rate. Must be greater than group_resolution\n
        '''
        self.__group_resolution = group_resolution
        self.__rate_interval = rate_interval

    def execute(self, input_streams: Sequence[Stream]):
        '''
        Starts convercence analyis.\n

        Parameters:\n
            in_streams : Stream
                Two time series streams to analyise, must contain same-interval atoms with 'close' key.
        '''
        # Prepare output_streams
        output_streams = [Stream(), Stream()]
        # Calculate rates ever rate_interval
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
                    operation=lambda atom: kh.lower_all_keys_deep(atom)
                ),
                GenericFilter(
                    inputs="atoms2",
                    outputs="lower_s2",
                    operation=lambda atom: kh.lower_all_keys_deep(atom)
                )
            ], EXEC_AND_PASS),
            FilterLayer([
                # Interpolation
                GroupFilter(
                    inputs="lower_s1",
                    outputs="grouped_s1",
                    resolution=self.__group_resolution
                ),
                GroupFilter(
                    inputs="lower_s2",
                    outputs="grouped_s2",
                    resolution=self.__group_resolution
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
                RateCalcFilter(
                    inputs=["align_s1", "align_s2"],
                    outputs=["o1", "o2"],
                    split_every=self.__rate_interval
                )
            ], EXEC_AND_PASS)
        ]).execute(source={"s1": input_streams[0], "s2": input_streams[1], "o1": output_streams[0], "o2": output_streams[1]})

        rates = convergence_net.state("rate", {})

        # Calculate average rate
        average_rate = 0
        for date, rate in rates.items():
            average_rate += rate
        if len(rates) > 0:
            average_rate /= len(rates)

        # Calculate variance
        variance = 0
        for date, rate in rates.items():
            variance += (rate - average_rate) ** 2
        if len(rates) > 0:
            variance /= len(rates)

        # Calculate probability samples

        samples_net = FilterNet(layers=[
            FilterLayer([
                # Rate as atoms
                MultipleGenericFiler(
                    inputs=["s1", "s2"],
                    outputs="rate",
                    operation=functools.partial(rate_atom_func, average_rate)
                )
            ], EXEC_AND_PASS),
            FilterLayer([
                # Sample density
                ThresholdFilter(
                    inputs="rate",
                    outputs="o",
                    price_keys=['close'],
                    step=lambda i: round(i*0.01, ndigits=4)
                )
            ], EXEC_AND_PASS)]).execute(source={"s1": output_streams[0], "s2": output_streams[1]})

        samples_dict = samples_net.state(key='thresholds', default={})

        return {"rates": rates, "average_rate": average_rate, "variance": variance, "samples": samples_dict}
