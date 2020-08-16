
from . import Analysis, Stream, Sequence
from ..filtering.filter_net import FilterNet, FilterLayer, EXEC_AND_PASS, BACK_IF_NO_OUTPUT
from ..filtering.filter import Filter, Any, Mapping
from ..filtering.filters.generic_filter import GenericFilter
from ..filtering.filters.interpolation_filter import IntradayInterpolationFilter
from ..filtering.filters.align_filter import AlignFilter
from ..utils import key_handler as kh, time_handler as th
from datetime import timedelta, datetime, timezone
import numpy as np


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


class DivergenceDensityFilter(Filter):

    def __init__(self,  inputs: Sequence[str], outputs: Sequence[str], avg_rate: float, price_key: str = 'close'):
        '''
        Parameters:\n
            inputs : Sequence[str]
                Input stream names.\n
            outputs : Sequence[str]
                Output stream names.\n
            avg_rate : float
                Average constant rate between the ticker streams.\n
            price_key : str
                Key that contains the price value.\n
        '''
        super().__init__(
            inputs=inputs,
            outputs=outputs,
            input_count=2,
            output_count=2
        )
        self.__avg_rate = avg_rate
        self.__price_key = price_key

    def setup(self, inputs: Sequence[Stream], outputs: Sequence[Stream], state: Mapping[str, Any]):
        # Call superclass setup
        super().setup(inputs, outputs, state)
        self.__state = state
        self.__atoms = [None, None]
        # TODO : more sensate state name
        self.__state['thresholds'] = dict()
        self.__next_stream = 0
        self.__threshold_iter = 0

    def _on_data(self, data: Any, index: int):
        '''
        Counts how many times the values gets over or under a number of thresholds
        '''
        self.__atoms[index] = data
        if self.__atoms[0] is not None and self.__atoms[1] is not None:
            # Calc rate
            rate = self.__atoms[0][self.__price_key] / self.__atoms[1][self.__price_key]
            # Calc percentage difference
            diff = (rate / self.__avg_rate) - 1
            # Round the diff to match the threshold
            thresh_diff = round(diff, ndigits=4)
            if(diff > 0):
                if(thresh_diff > self.__threshold_iter):
                    for threshold in np.arange(max(self.__threshold_iter, 0.0001), thresh_diff + 0.0001, 0.0001):
                        threshold = round(threshold, ndigits=4)
                        # Set 0 if value is not in dict
                        self.__state['thresholds'].setdefault(threshold, 0)
                        # Update value
                        self.__state['thresholds'][threshold] += 1
                    # Update threshold
                    self.__threshold_iter = thresh_diff
            elif(diff < 0):
                if(thresh_diff < self.__threshold_iter):
                    for threshold in np.arange(-min(self.__threshold_iter, -0.0001), -thresh_diff + 0.0001, 0.0001):
                        threshold = -round(threshold, ndigits=4)
                        # Set 0 if value is not in dict
                        self.__state['thresholds'].setdefault(threshold, 0)
                        # Update value
                        self.__state['thresholds'][threshold] += 1
                    # Update threshold
                    self.__threshold_iter = thresh_diff
            # Update next stream iterator
            self.__next_stream = 0
            # Clear atoms buffer
            self.__atoms[0] = self.__atoms[1] = None
        else:
            self.__next_stream = 1
        self._push_data(data, index=index)

    def _input_check_order(self) -> Sequence[int]:
        return [self.__next_stream]


class ConvergenceAnalysis(Analysis):
    '''
    Calculates the ratio between two time series in different periods and returns its value and its variance.
    '''

    def __init__(self, rate_interval: timedelta = timedelta(seconds=3600)):
        '''
        Parameters:\n
            rate_interval : timedelta
                Interval of time where to calculate the average rate.
        '''
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
                IntradayInterpolationFilter(
                    inputs="lower_s1",
                    outputs="interp_s1",
                    interp_keys=['close'],
                    constant_keys=["ticker"],
                    target_gap_seconds=60
                ),
                IntradayInterpolationFilter(
                    inputs="lower_s2",
                    outputs="interp_s2",
                    interp_keys=['close'],
                    constant_keys=["ticker"],
                    target_gap_seconds=60
                )
            ], EXEC_AND_PASS),
            FilterLayer([
                # Align datetime
                AlignFilter(
                    inputs=["interp_s1", "interp_s2"],
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

        # Calculate probability density

        density_net = FilterNet(layers=[
            FilterLayer([
                # Divergence density
                DivergenceDensityFilter(
                    inputs=["s1", "s2"],
                    outputs=["o1", "o2"],
                    avg_rate=average_rate,
                    price_key='close'
                )
            ], EXEC_AND_PASS)]).execute(source={"s1": output_streams[0], "s2": output_streams[1]})

        density_dict = density_net.state(key='thresholds', default={})

        return {"rates": rates, "average_rate": average_rate, "variance": variance, "density": density_dict}
