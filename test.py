from otri.downloader.alphavantage_downloader import AVDownloader
from otri.downloader.yahoo_downloader import YahooDownloader
from datetime import date
from otri.config import Config

if __name__ == "__main__":

    category = "MGP"
    req_type = "Prezzi"
    start_date = date(2020,4,1)
    end_date = date(2020,4,1)
    downloader = GMEDownloader()
    downloader.download_between_dates(category=category,req_type=req_type, start=start_date, end=end_date)
