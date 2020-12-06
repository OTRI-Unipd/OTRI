import unittest
from otri.downloader import TickerSplitterComp, ParamValidatorComp, TickerExtractorComp


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

class ParamValidatorCompTest(unittest.TestCase):
    '''
    Tests parameter validation functionalities.
    '''

    WRONG_NUMBER_PARAM = {'number': 15}
    OK_NUMBER_PARAM = {'number' : 8}
    WRONG_INTERVAL_PARAM = {'interval': '12m'}
    OK_INTERVAL_PARAM = {'interval': '1h'}
    POSSIBLE_INTERVALS = ["1m", "5m", "15m", "1h", "1d"]

    @staticmethod
    def val_method(value):
            if value > 10:
                raise ValueError("'number' param's value too high")

    def test_custom_validation_exception(self):
        '''
        Asserts that if a parameter is wrong an ValueError is raised.
        '''
        with self.assertRaises(expected_exception=ValueError):
            ParamValidatorComp(validator_mapping={'number': self.val_method}).prepare(**self.WRONG_NUMBER_PARAM)

    def test_custom_validation_untouched_param(self):
        '''
        Checks if everything is ok parameters are still the same.
        '''
        self.assertEqual(self.OK_NUMBER_PARAM, ParamValidatorComp(validator_mapping={'number': self.val_method}).prepare(**self.OK_NUMBER_PARAM))

    def test_match_validation_exception(self):
        '''
        Asserts that if a parameter is wrong an ValueError is raised by the match validation method.
        '''
        with self.assertRaises(expected_exception=ValueError):
            interval_match_validator = ParamValidatorComp.match_param_validation(key='interval', possible_values=self.POSSIBLE_INTERVALS)
            ParamValidatorComp(validator_mapping={'interval': interval_match_validator}).prepare(**self.WRONG_INTERVAL_PARAM)

    def test_match_validation_untouched_param(self):
        '''
        Checks if everything is ok parameters are still the same.
        '''
        interval_match_validator = ParamValidatorComp.match_param_validation(key='interval', possible_values=self.POSSIBLE_INTERVALS)
        self.assertEqual(self.OK_INTERVAL_PARAM, ParamValidatorComp(validator_mapping={'interval': interval_match_validator}).prepare(**self.OK_INTERVAL_PARAM))

class TickerExtractorCompTest(unittest.TestCase):
    '''
    Tests ticker extactor functionalities and its checks.
    '''

    TICKER_GROUP = ['A']
    EMPTY_TICKER_GROUP = []
    WRONG_TICKER_GROUP = ['A','B']
    TICKER_OUTPUT = 'A'

    def setUp(self):
        self.comp = TickerExtractorComp(ticker_coll_name='ticker_group', ticker_name='ticker')

    def test_working(self):
        self.assertEqual(self.TICKER_OUTPUT, self.comp.prepare(ticker_group=self.TICKER_GROUP)['ticker'])
    
    def test_empty_group(self):
        with self.assertRaises(expected_exception=ValueError):
            self.comp.prepare(ticker_group=self.EMPTY_TICKER_GROUP)

    def test_empty_group(self):
        with self.assertRaises(expected_exception=ValueError):
            self.comp.prepare(ticker_group=self.WRONG_TICKER_GROUP)


# TODO: Ticker group handler tests

# TODO: Request component tests
