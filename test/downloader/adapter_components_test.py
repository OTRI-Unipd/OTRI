import unittest
from otri.downloader import SubAdapter, TickerSplitterComp, ParamValidatorComp, SubAdapter, MappingComp, AdapterComponent


class TickerSplitterCompTest(unittest.TestCase):
    '''
    Tests ticker list splitting functionalities.
    '''

    TICKERS = {'tickers': ['A', 'AA', 'ABC', 'ACB']}
    EXPECTED_1 = [['A'], ['AA'], ['ABC'], ['ACB']]
    EXPECTED_2 = [['A', 'AA'], ['ABC', 'ACB']]
    EXPECTED_3 = [['A', 'AA', 'ABC'], ['ACB']]
    EXPECTED_MAX = [['A', 'AA', 'ABC', 'ACB']]

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
    # Custom method
    WRONG_NUMBER_PARAM = {'number': 15}
    OK_NUMBER_PARAM = {'number': 8}
    # Match method
    WRONG_INTERVAL_PARAM = {'interval': '12m'}
    OK_INTERVAL_PARAM = {'interval': '1h'}
    POSSIBLE_INTERVALS = ["1m", "5m", "15m", "1h", "1d"]
    # Datetime method
    WRONG_DT_PARAM_1 = {'start': '2020-20-12 05:02'}
    WRONG_DT_PARAM_2 = {'start': '2020-08-12 05:02:20'}
    DT_FORMAT = "%Y-%m-%d %H:%M"
    OK_DT_PARAM = {'start': '2020-08-12 09:25'}

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

    def test_match_validation_exception(self):
        '''
        Asserts that if a parameter is wrong an ValueError is raised by the match validation method.
        '''
        with self.assertRaises(expected_exception=ValueError):
            interval_match_validator = ParamValidatorComp.match_param_validation(key='interval', possible_values=self.POSSIBLE_INTERVALS)
            ParamValidatorComp(validator_mapping={'interval': interval_match_validator}).prepare(**self.WRONG_INTERVAL_PARAM)

    def test_dt_param_exception(self):
        dt_validator = ParamValidatorComp.datetime_param_validation(key='start', dt_format=self.DT_FORMAT, required=True)
        with self.assertRaises(expected_exception=ValueError):
            ParamValidatorComp(validator_mapping={'start': dt_validator}).prepare(**self.WRONG_DT_PARAM_1)
            ParamValidatorComp(validator_mapping={'start': dt_validator}).prepare(**self.WRONG_DT_PARAM_2)


class MappingComponentTest(unittest.TestCase):

    PARAMS = {'type': 'timeseries'}
    MAPPING = {'market/timeseries': ['timeseries', 'time series', 'time-series']}
    RENAMED_PARAM = {'type': 'market/timeseries'}

    def test_working(self):
        component = MappingComp(key='type', value_mapping=self.MAPPING, required=True)
        self.assertEqual(self.RENAMED_PARAM, component.prepare(**self.PARAMS))

    def test_missing_required_key_exception(self):
        component = MappingComp(key='missing-key', value_mapping=self.MAPPING, required=True)
        with self.assertRaises(expected_exception=ValueError):
            component.prepare(**self.PARAMS)

    def test_missing_required_key_NO_exception(self):
        component = MappingComp(key='missing-key', value_mapping=self.MAPPING, required=False)
        component.prepare(**self.PARAMS)

    # TODO: further testing


class SubAdapterTest(unittest.TestCase):

    class CustomComponent(AdapterComponent):

        def prepare(self, **kwargs):
            kwargs['A'] = 1
            return kwargs

        def retrieve(self, data_stream, **kwargs):
            kwargs['B'] = 2
            return kwargs

        def atomize(self, **kwargs):
            kwargs['C'] = 3
            return kwargs

    def setUp(self):
        self.component = SubAdapter(components=[
            SubAdapterTest.CustomComponent(),
        ], list_name='list_name', out_name='out_name')

    def test_missing_list_param(self):
        kwargs = {'nothing': 'really'}
        with self.assertRaises(expected_exception=ValueError):
            self.component.retrieve(data_stream=None, **kwargs)

    def test_list_param_not_list(self):
        kwargs = {'list_name': 1}
        with self.assertRaises(expected_exception=ValueError):
            self.component.retrieve(data_stream=None, **kwargs)

    def test_components_see_element(self):
        kwargs = {'list_name': [1, 2, 3]}
        # Check that the last element seen is in the out_name param
        self.assertEqual(
            self.component.retrieve(data_stream=None, **kwargs)['out_name'], 3
        )

    def test_component_is_called(self):
        kwargs = {'list_name': [1, 2, 3]}
        out_kwargs = self.component.retrieve(data_stream=None, **kwargs)
        self.assertEqual(
            out_kwargs['A'], 1
        )
        self.assertEqual(
            out_kwargs['B'], 2
        )

# TODO: Request component tests
