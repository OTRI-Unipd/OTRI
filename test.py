from download.alphavantage_downloader import AVDownloader
from datetime import date
from config import Config

if __name__ == "__main__":
    dw = AVDownloader(Config.get_config("alphavantage_api_key"))
    print(dw.download_between_dates("A",date(2020,4,21),date(2020,4,23)))