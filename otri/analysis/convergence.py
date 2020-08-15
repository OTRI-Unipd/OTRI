
from . import Analysis, Stream, Sequence
from ..filtering.filter_net import FilterNet, FilterLayer, EXEC_AND_PASS, BACK_IF_NO_OUTPUT
from ..filtering.filter import Filter, Any, Mapping
from ..filtering.filters.generic_filter import GenericFilter
from ..filtering.filters.interpolation_filter import IntradayInterpolationFilter
from ..utils import key_handler as kh


class RateCalcFilter(Filter):

    def __init__(self, inputs: Sequence[str], outputs: Sequence[str]):
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
        self.__state = state
        self.__atoms = (None, None)
        self.__rate_sum = 0
        self.__counter = 0

    def _on_data(self, data: Any, index: int):
        '''
        Applies the operation on the atom then pushes it into the output
        '''
        self.__atoms[index] = data
        # Update atoms counter (needed for average)
        self.__counter = self.__counter + 1
        if self.__atoms.get(0, None) is not None and self.__atoms.get(1, None) is not None:
            # Atom rate
            rate = self.__atoms[0]/self.__atoms[1]
            # Rate sum
            self.__rate_sum += rate
            # Update average rate
            self.__state['rate'] = self.__rate_sum/self.__counter
            # Remove atoms
            del self.__atoms[0], self.__atoms[1]
        self._push_data(data)

    def _input_check_order(self) -> Sequence:
        '''
        Defines the order for the inputs to be checked.
        '''
        return [0] if self.__counter % 2 == 0 else [1]


class ConvergenceAnalysis(Analysis):
    '''
    Calculates the ratio between two time series in different periods and returns its value and its variance.
    '''

    def __init__(self):
        '''

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
                # To Lowercase
                GenericFilter(
                    inputs="s1",
                    outputs="lower_s1",
                    operation=lambda atom: kh.lower_all_keys_deep(atom)
                ),
                GenericFilter(
                    inputs="s2",
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
            ], BACK_IF_NO_OUTPUT),
            FilterLayer([
                # Interpolation
                IntradayInterpolationFilter(
                    inputs=["interp_s1", "interp_s2"],
                    outputs="output"
                )
            ], EXEC_AND_PASS)
        ]).execute(source={"s1": input_streams[0], "s2": input_streams[1]})
        return convergence_net.state("rate", 0)
