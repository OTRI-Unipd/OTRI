import unittest
from datetime import datetime, timedelta

from otri.downloader.tradier import (TradierMetadataAdapter,
                                     TradierTimeseriesAdapter)
from otri.filtering.stream import LocalStream
from otri.utils import config


class TradierTimeseriesAdapterTest(unittest.TestCase):
    '''
    Tests tradier timeseries adapter.
    '''

    TICKERS = ['AAPL', 'MSFT']
    ATOM_KEYS = ['open', 'close', 'high', 'low', 'last', 'datetime', 'volume', 'vwap', 'provider', 'ticker']

    def test_generic_timeseries(self):
        adapter = TradierTimeseriesAdapter(user_key=config.get_value("tradier_api_key"))
        end_dt = datetime.now().strftime("%Y-%m-%d %H:%M")
        start_dt = (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d %H:%M")  # timeseries data is available max 10 days earlier
        # Stock market is never closed more than 3 days in a row (fri-sat-sun or sat-sun-mon)
        o_stream = LocalStream()
        adapter.download(o_stream, tickers=self.TICKERS, interval="1min", start=start_dt, end=end_dt)  # Assert doesn't raise exception
        self.assertTrue(o_stream.has_next())
        # Pop the first atom and check that the required keys are in it
        example_atom = o_stream.pop()
        for key in self.ATOM_KEYS:
            self.assertIn(key, example_atom)


class TradierMeteadataAdapterTest(unittest.TestCase):
    '''
    Tests tradier metadata adapter.
    '''

    TICKERS = ['AAPL', 'MSFT', 'GOOG', 'AMZN', 'FB', 'GOOGL', 'INTC', 'NVDA', 'TSLA']
    ATOM_KEYS = ['ticker', 'description', 'exchange', 'type', 'root_symbols']

    def test_multi_ticker_metadata(self):
        adapter = TradierMetadataAdapter(user_key=config.get_value("tradier_api_key"))
        o_stream = LocalStream()
        adapter.download(o_stream, tickers=self.TICKERS)
        self.assertTrue(o_stream.has_next())
        # Pop the first atom and check that the required keys are in it
        example_atom = o_stream.pop()
        for key in self.ATOM_KEYS:
            self.assertIn(key, example_atom)

    def test_single_ticker_metadata(self):
        adapter = TradierMetadataAdapter(user_key=config.get_value("tradier_api_key"))
        o_stream = LocalStream()
        adapter.download(o_stream, tickers=[self.TICKERS[0]])
        self.assertTrue(o_stream.has_next())
        # Pop the first atom and check that the required keys are in it
        example_atom = o_stream.pop()
        for key in self.ATOM_KEYS:
            self.assertIn(key, example_atom)
