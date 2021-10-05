import unittest
from datetime import datetime, timedelta

from otri.downloader.yahoo import YahooMetadataAdapter, YahooTimeseriesAdapter


class YahooTimeseriesAdapterTest(unittest.TestCase):

    TICKERS = ['AAPL', 'MSFT']
    ATOM_KEYS = ['open', 'close', 'high', 'low', 'volume', 'ticker', 'interval']

    def test_generic_timeseries(self):
        adapter = YahooTimeseriesAdapter()
        end_dt = datetime.now().strftime("%Y-%m-%d %H:%M")
        # timeseries data is available max 10 days earlier
        start_dt = (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d %H:%M")
        # Stock market is never closed more than 3 days in a row (fri-sat-sun or sat-sun-mon)
        output = adapter.download(tickers=self.TICKERS, interval="1m", start=start_dt,
                                  end=end_dt)  # Assert doesn't raise exception
        self.assertTrue(len(output) > 0)
        # Pop the first atom and check that the required keys are in it
        example_atom = output[0]
        for key in self.ATOM_KEYS:
            self.assertIn(key, example_atom)
        # Check that the tickers match
        self.assertEqual(example_atom['ticker'], self.TICKERS[0])

    def test_interval(self):
        adapter = YahooTimeseriesAdapter()
        output = adapter.download(tickers=self.TICKERS, interval="1m",
                                  range="5d")  # Assert doesn't raise exception
        self.assertTrue(len(output) > 0)
        # Pop the first atom and check that the required keys are in it
        example_atom = output[0]
        for key in self.ATOM_KEYS:
            self.assertIn(key, example_atom)


class YahooMetadataAdapterTest(unittest.TestCase):

    TICKERS = ['AAPL', 'MSFT']
    ATOM_KEYS = ['ticker', 'exchange', 'longName', 'sector', 'enterpriseValue', 'shortRatio']

    def test_generic_metadata(self):
        adapter = YahooMetadataAdapter()
        output = adapter.download(tickers=self.TICKERS)  # Assert doesn't raise exception
        self.assertTrue(len(output) == 2)
        # Pop the first atom and check that the required keys are in it
        example_atom = output[0]
        for key in self.ATOM_KEYS:
            self.assertIn(key, example_atom)
