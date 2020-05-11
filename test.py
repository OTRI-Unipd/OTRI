from otri.filtering.filter_list import FilterList, FilterLayer
from otri.filtering.stream import Stream
from otri.filtering.filters.interpolation_filter import InterpolationFilter
from otri.filtering.filters.multiplier_filter import MultiplierFilter
from otri.filtering.filters.summer_filter import SummerFilter
from otri.downloader.yahoo_downloader import YahooDownloader, date
from otri.downloader.alphavantage_downloader import AVDownloader
import matplotlib.pyplot as plt
import time


def on_atom(atom):
    #times.append(th.str_to_datetime(atom['datetime']).timestamp())
    closes.append(atom['close'])


def on_finished():
    end_time = time.time()
    seconds = end_time - start_time
    print("Took {} seconds".format(seconds))
    plt.plot(closes)
    plt.xticks(rotation=90)
    plt.show()


if __name__ == "__main__":
    times = list()
    closes = list()
    #downloader = AVDownloader(Config.get_config("alphavantage_api_key"))
    downloader = YahooDownloader()
    downloaded_data = downloader.download_between_dates(
        ticker="IBM", start=date(2020, 5, 5), end=date(2020, 5, 9), interval="1m")
    atom_stream = Stream(downloaded_data['atoms'], is_closed=True)

    interp_filter = InterpolationFilter(input_stream=atom_stream, keys_to_change=["open", "high", "low", "close"], target_interval="minutes")
    f_layer_interp = FilterLayer([interp_filter])
    
    #avg_filter = AverageFilter(interp_filter.get_output_stream())
    #f_layer_avg = FilterLayer([avg_filter])

    #f_list_1 = FilterList([f_layer_interp, f_layer_avg])

    start_time = time.time()
    #f_list_1.execute(on_atom, on_finished)
