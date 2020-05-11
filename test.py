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
    f_list = FilterList()

    f_layer_1 = FilterLayer()
    filter_1 = InterpolationFilter(input_stream=atom_stream, keys_to_change=["open", "high", "low", "close"], target_interval="minutes")
    f_layer_1.append(filter_1)

    f_layer_2 = FilterLayer()
    filter_2 = SummerFilter(input_stream=filter_1.get_output_stream(0), keys_constants={"open": -120, "close":-123})
    f_layer_2.append(filter_2)

    f_layer_3 = FilterLayer()
    filter_3 = MultiplierFilter(input_stream=filter_2.get_output_stream(0), keys_to_change=["open", "high", "low", "close"], distance=15)
    f_layer_3.append(filter_3)

    f_list.add_layer(f_layer_1)
    f_list.add_layer(f_layer_2)
    f_list.add_layer(f_layer_3)
    start_time = time.time()
    f_list.execute(on_atom, on_finished)
