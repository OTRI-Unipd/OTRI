from otri.filtering.filter_list import FilterList, FilterLayer, Filter
from otri.filtering.stream import Stream
from otri.filtering.filters.interpolation_filter import InterpolationFilter

def on_atom(atom):
    print("on_atom: {}".format(atom))

if __name__ == "__main__":
    source_stream_1 = Stream([{
        "datetime":"2020-04-21 20:00:00.000",
        "open": 10,
        "high": 10,
        "low": 10,
        "close": 10,
    },
    {
        "datetime":"2020-04-22 00:01:00.000",
        "open": 2,
        "high": 2,
        "low": 2,
        "close": 2,
    }])
    f_list = FilterList([source_stream_1])
    f_layer_1 = FilterLayer()
    f_layer_1.append(InterpolationFilter(input_stream=f_list.get_stream_output(),keys_to_change=["open","high","low","close"],target_interval="minutes"))
    f_list.add_layer(f_layer_1)
    f_list.execute(on_atom)
