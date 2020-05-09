from otri.filtering.filter_list import FilterList, FilterLayer
from otri.filtering.stream import Stream
from otri.filtering.filters.interpolation_filter import InterpolationFilter
from otri.downloader.yahoo_downloader import YahooDownloader, date
from otri.downloader.alphavantage_downloader import AVDownloader
from otri.config import Config
from otri.utils import time_handler as th
import matplotlib.pyplot as plt


def on_atom(atom):
    times.append(th.str_to_datetime(atom['datetime']).timestamp())
    closes.append(atom['close'])


def on_finished():
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
    f_list = FilterList([Stream(downloaded_data['atoms'])])
    f_layer_1 = FilterLayer()
    f_layer_1.append(InterpolationFilter(input_stream=f_list.get_stream_output(
    ), keys_to_change=["open", "high", "low", "close"], target_interval="minutes"))
    f_list.add_layer(f_layer_1)
    f_list.execute(on_atom, on_finished)
