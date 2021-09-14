import unittest
from datetime import datetime, timedelta

from otri.downloader.yahoo import YahooTimeseriesAdapter
from otri.filtering.stream import LocalStream


class YahooTimeseriesAdapterTest(unittest.TestCase):

    TICKERS = ['AAPL', 'MSFT']
    ATOM_KEYS = ['open', 'close', 'high', 'low', 'volume', 'ticker', 'interval']

    def test_generic_timeseries(self):
        adapter = YahooTimeseriesAdapter()
        end_dt = datetime.now().strftime("%Y-%m-%d %H:%M")
        start_dt = (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d %H:%M")  # timeseries data is available max 10 days earlier
        # Stock market is never closed more than 3 days in a row (fri-sat-sun or sat-sun-mon)
        o_stream = LocalStream()
        adapter.download(o_stream, tickers=self.TICKERS, interval="1m", start=start_dt, end=end_dt)  # Assert doesn't raise exception
        self.assertTrue(o_stream.has_next())
        # Pop the first atom and check that the required keys are in it
        example_atom = o_stream.pop()
        for key in self.ATOM_KEYS:
            self.assertIn(key, example_atom)
