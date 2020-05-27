"""
File used to test styles of the filtering pattern classes.
"""

from otri.filtering.filter_list import FilterList, FilterLayer
from otri.filtering.stream import Stream
from otri.filtering.filters.interpolation_filter import InterpolationFilter
from otri.filtering.filters.generic_filter import GenericFilter
from otri.filtering.filters.math_filter import MathFilter


def correlation1(ticker1_stream: Stream, ticker2_stream: Stream, c: int) -> float:
    """
    Calcs correlation between two tickers using the delta tecnique.
    """
    corr_filter_list = FilterList([
        FilterLayer(
            filters=[
                # Tuple extractors
                GenericFilter(
                    input="input1",
                    output="ticker1_atoms",
                    operation=lambda db_tuple: db_tuple[0]
                ),
                GenericFilter(
                    input="input2",
                    output="ticker2_atoms",
                    operation=lambda db_tuple: db_tuple[0]
                )
            ],
            policy=EXEC_AND_PASS
        ),
        FilterLayer(
            filters=[
                # Interpolation
                InterpolationFilter(
                    input="ticker1_atoms",
                    output="ticker1_interp",
                    keys=KEYS_TO_CHANGE,
                    target_interval="minutes"
                ),
                InterpolationFilter(
                    input="ticker2_atoms",
                    output="ticker2_interp",
                    keys=KEYS_TO_CHANGE,
                    target_interval="minutes"
                )
            ],
            policy=BACK_IF_NO_OUTPUT
        ),
        FilterLayer(
            filters=[
                # Delta calculation
                PhaseDeltaFilter(
                    input="ticker1_interp",
                    output="ticker1_delta"
                    keys=KEYS_TO_CHANGE,
                    distance=distance
                ),
                PhaseDeltaFilter(
                    input="ticker2_interp",
                    output="ticker2_delta"
                    keys=KEYS_TO_CHANGE,
                    distance=distance
                )
            ],
            policy=BACK_IF_NO_OUTPUT
        ),
        FilterLayer(
            filters=[
                MultiStreamMathFilter(
                    input=["ticker1_delta", "ticker2_delta"],
                    output="delta_mult",
                    operation=lambda delta1, delta2: delta1 * delta2
                )
            ],
            policy=BACK_IF_NO_OUTPUT
        ),
        FilterLayer(
            filters=[
                StatisticsFilter(
                    input="delta_mult",
                    output="output"
                ).calc_avg("average_delta_mult")
            ],
            policy=BACK_IF_NO_OUTPUT
        )
    ]).execute(input={"input1": ticker1_stream, "input2": ticker2_stream})

    return corr_filter_list.get_state("average_delta_mult")

def correlation2(ticker1_stream: Stream, ticker2_stream: Stream, c: int) -> float:
    """
    Calcs correlation between two tickers using the delta tecnique.
    """
    corr_f_list = FilterList()
    corr_f_list.add_layer(
        filters=[
            # Tuple extractors
            GenericFilter(
                input="input1",
                output="ticker1_atoms",
                operation=lambda db_tuple: db_tuple[0]
            ),
            GenericFilter(
                input="input2",
                output="ticker2_atoms",
                operation=lambda db_tuple: db_tuple[0]
            )
        ],
        policy=EXEC_AND_PASS
    )
    corr_f_list.add_layer(
        filters=[
            # Interpolation
            InterpolationFilter(
                input="ticker1_atoms",
                output="ticker1_interp",
                keys=KEYS_TO_CHANGE,
                target_interval="minutes"
            ),
            InterpolationFilter(
                input="ticker2_atoms",
                output="ticker2_interp",
                keys=KEYS_TO_CHANGE,
                target_interval="minutes"
            )
        ],
        policy=BACK_IF_NO_OUTPUT
    )
    corr_f_list.add_layer(
        filters=[
            # Delta calculation
            PhaseDeltaFilter(
                input="ticker1_interp",
                output="ticker1_delta"
                keys=KEYS_TO_CHANGE,
                distance=distance
            ),
            PhaseDeltaFilter(
                input="ticker2_interp",
                output="ticker2_delta"
                keys=KEYS_TO_CHANGE,
                distance=distance
            )
        ],
        policy=BACK_IF_NO_OUTPUT
    )
    corr_f_list.add_layer(
        filters=[
            MultiStreamMathFilter(
                input=["ticker1_delta", "ticker2_delta"],
                output="delta_mult",
                operation=lambda delta1, delta2: delta1 * delta2
            )
        ],
        policy=BACK_IF_NO_OUTPUT
    )

    corr_f_list.add_layer(
        filters=[
            StatisticsFilter(
                input="delta_mult",
                output="output"
            ).calc_avg("average_delta_mult")
        ],
        policy=BACK_IF_NO_OUTPUT
    )

    corr_f_list.execute()
