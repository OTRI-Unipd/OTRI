from download.yahoo_downloader import YahooDownloader
from datetime import date

if __name__ == "__main__":
    dw = YahooDownloader()
    print(dw.download_between_dates("AAPL",date(2020,4,22),date(2020,4,23)))