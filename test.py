from otri.filtering.filter_list import FilterList, FilterLayer
from otri.filtering.stream import Stream
from otri.filtering.filters.interpolation_filter import InterpolationFilter
from otri.filtering.filters.multiplier_filter import MultiplierFilter
from otri.filtering.filters.summer_filter import SummerFilter
from otri.filtering.filters.average_filter import AverageFilter
from otri.downloader.yahoo_downloader import YahooDownloader, date
from otri.downloader.alphavantage_downloader import AVDownloader
import matplotlib.pyplot as plt
import time


def on_atom(atom):
    # times.append(th.str_to_datetime(atom['datetime']).timestamp())
    closes.append(atom['close'])


def on_finished():
    end_time = time.time()
    seconds = end_time - start_time
    print("Took {} seconds".format(seconds))
    plt.plot(closes)
    plt.xticks(rotation=90)
    plt.show()

KEYS_TO_CHANGE = ("open", "high", "low", "close")

if __name__ == "__main__":
    times = list()
    closes = list()
    #downloader = AVDownloader(Config.get_config("alphavantage_api_key"))
    downloader = YahooDownloader()
    downloaded_data = downloader.download_between_dates(
        ticker="IBM", start=date(2020, 5, 5), end=date(2020, 5, 9), interval="1m")
    atom_stream = Stream(downloaded_data['atoms'], is_closed=True)

    # Filter list 1

    interp_filter = InterpolationFilter(
        input_stream=atom_stream,
        keys_to_change=KEYS_TO_CHANGE,
        target_interval="minutes"
    )
    f_layer_interp = FilterLayer([interp_filter])

    avg_filter = AverageFilter(
        input_stream=interp_filter.get_output_stream(0),
        keys=KEYS_TO_CHANGE
    )
    f_layer_avg = FilterLayer()
    f_layer_avg.append(avg_filter)

    f_list_1 = FilterList([f_layer_interp, f_layer_avg])
    f_list_1.execute()
    print("avgs:{}\nsums:{}".format(
        avg_filter.get_avgs(), avg_filter.get_sums()
    ))

    # Filter list 2
    summer_filter = SummerFilter(
        input_stream=avg_filter.get_output_stream(0),
        keys_constants={k: -v for k,v in avg_filter.get_avgs().items()}
    )
    f_layer_sum = FilterLayer([summer_filter])

    mul_filter = MultiplierFilter(
        input_stream=summer_filter.get_output_stream(0),
        keys_to_change=KEYS_TO_CHANGE,
        distance=1
    )
    f_layer_mul = FilterLayer([mul_filter])

    f_list_2 = FilterList([f_layer_sum, f_layer_mul])

    start_time = time.time()
    f_list_2.execute(on_atom, on_finished)
