from download.alphavantage_downloader import AVDownloader
from download.yahoo_downloader import YahooDownloader
from datetime import date
from config import Config

if __name__ == "__main__":

    ticker = "GOOG"

    print("Ticker: ", ticker)

    dw = AVDownloader(Config.get_config("alphavantage_api_key"))
    data = dw.download_between_dates(ticker, date(
        2020, 4, 19), date(2020, 4, 25), interval="1m")
    print("AV min: {} max: {} len: {}".format(data['atoms'][len(
        data['atoms'])-1]['datetime'], data['atoms'][0]['datetime'], len(data['atoms'])))

    dw2 = YahooDownloader()
    data2 = dw2.download_between_dates(ticker=ticker, start_date=date(
        2020, 4, 19), end_date=date(2020, 4, 25), interval="1m")
    print("YF min: {} max: {} len: {}".format(data2['atoms'][0]['datetime'], data2['atoms'][len(
        data2['atoms'])-1]['datetime'], len(data2['atoms'])))
