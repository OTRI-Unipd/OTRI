
from . import Analysis, Stream, Sequence
from ..filtering.filter_net import FilterNet, FilterLayer, EXEC_AND_PASS, BACK_IF_NO_OUTPUT
from ..filtering.filter import Filter, Any, Mapping
from ..filtering.filters.generic_filter import GenericFilter
from ..filtering.filters.interpolation_filter import IntradayInterpolationFilter
from ..filtering.filters.align_filter import AlignFilter
from ..utils import key_handler as kh, time_handler as th
from datetime import timedelta, datetime, timezone


class RateCalcFilter(Filter):

    def __init__(self, inputs: Sequence[str], outputs: Sequence[str], split_every: timedelta = timedelta(seconds=3600)):
        '''
        Parameters:\n
            inputs : str
                Input stream name.\n
            outputs : str
                Output stream name.\n
        '''
        super().__init__(
            inputs=inputs,
            outputs=outputs,
            input_count=2,
            output_count=2
        )
        self.__split_every = split_every

    def setup(self, inputs: Sequence[Stream], outputs: Sequence[Stream], state: Mapping[str, Any]):
        '''
        Used to save references to streams and reset variables.\n
        Called once before the start of the execution in FilterNet.\n

        Parameters:\n
            inputs, outputs : Sequence[Stream]
                Ordered sequence containing the required input/output streams gained from the FilterNet.\n
            state : Mapping[str, Any]
                Dictionary containing states to output.\n
        '''
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
        Applies the operation on the atom then pushes it into the output
        '''
        self.__atoms[index] = data
        # Update atoms counter (for input stream selection)
        self.__counter += 1
        if self.__atoms[0] is not None and self.__atoms[1] is not None:
            # Update interval atoms counter
            self.__interval_atoms_counter += 1
            # Atom rate
            rate = self.__atoms[0]['close']/self.__atoms[1]['close']
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
        self._push_data(data)

    def _input_check_order(self) -> Sequence[int]:
        '''
        Defines the order for the inputs to be checked.
        '''
        return [0] if self.__counter % 2 == 0 else [1]


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

    def execute(self, input_streams: Sequence[Stream]):
        '''
        Starts convercence analyis.\n

        Parameters:\n
            in_streams : Stream
                Two time series streams to analyise, must contain same-interval atoms with 'close' key.
        '''
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
                    outputs=["output1", "output2"]
                )
            ], EXEC_AND_PASS)
        ]).execute(source={"s1": input_streams[0], "s2": input_streams[1]})

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

        return {"rates": rates, "average_rate": average_rate, "variance": variance}
