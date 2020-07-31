from otri.downloader.yahoo_downloader import YahooOptionsDW
from otri.utils import key_handler as kh
import yfinance as yf
import json

if __name__ == "__main__":
   #msft = yf.Ticker("MSFT")
   #options_expirations = msft.options
   #print(options_expirations)
   #calls = json.loads(msft.option_chain(options_expirations[0]).calls.to_json(
   #   orient="table"))  # TODO: Ogni quanto vanno scaricate?
   #del calls['schema']
   #print(json.dumps(calls, indent=4))

   #yf_data = yf.download("MSFT200724C00210000", start="2020-07-14", end="2020-07-21",
   #                     interval="1m", round=False, progress=False, prepost=True)
   #json_data = json.loads(yf_data.to_json(orient="table"))
   #print(json.dumps(json_data, indent=4))

   downloader = YahooOptionsDW()
   data = downloader.get_chain("MSFT",downloader.get_expirations("MSFT")[0], "calls")
   print(json.dumps(data, indent=4))