import yfinance as yf
import json

if __name__ == "__main__":
   msft = yf.Ticker("MSFT")
   options_expirations = msft.options
   print(options_expirations)
   calls = json.loads(msft.option_chain(options_expirations[0]).calls.to_json(orient="table")) #TODO: Ogni quanto vanno scaricate?
   del calls['schema']
   print(json.dumps(calls,indent=4))

   apple_option = yf.Ticker("AAPL200821C00360000");
   print(apple_option.history())