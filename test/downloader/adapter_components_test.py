import unittest
from otri.downloader import TickerSplitterComp


class TickerSplitterCompTest(unittest.TestCase):
    '''
    Tests ticker list splitting functionalities.
    '''

    TICKERS = {'tickers': ['A', 'AA', 'ABC', 'ACB']}
    EXPECTED_1 = [['A'], ['AA'], ['ABC'], ['ACB']]
    EXPECTED_2 = [['A', 'AA'], ['ABC', 'ACB']]
    EXPECTED_3 = [['A', 'AA', 'ABC'],['ACB']]
    EXPECTED_MAX = [['A','AA', 'ABC', 'ACB']]

    def test_max_1(self):
        self.assertEqual(self.EXPECTED_1, TickerSplitterComp(max_count=1).prepare(**self.TICKERS)['ticker_groups'])
    
    def test_max_2(self):
        self.assertEqual(self.EXPECTED_2, TickerSplitterComp(max_count=2).prepare(**self.TICKERS)['ticker_groups'])
    
    def test_max_3(self):
        self.assertEqual(self.EXPECTED_3, TickerSplitterComp(max_count=3).prepare(**self.TICKERS)['ticker_groups'])

    def test_max_4(self):
        self.assertEqual(self.EXPECTED_MAX, TickerSplitterComp(max_count=4).prepare(**self.TICKERS)['ticker_groups'])

    def test_max_100(self):
        self.assertEqual(self.EXPECTED_MAX, TickerSplitterComp(max_count=100).prepare(**self.TICKERS)['ticker_groups'])
