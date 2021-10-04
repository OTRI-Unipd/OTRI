import unittest

from otri.downloader import (AdapterComponent, ChunkerComp, ParamValidatorComp,
                             SubAdapter)

from otri.downloader.validators import match_param_validation, datetime_param_validation


class ChunkerCompTest(unittest.TestCase):
    '''
    Tests ticker list splitting functionalities.
    '''

    TICKERS = {'symbols': ['A', 'AA', 'ABC', 'ACB']}
    EXPECTED_1 = [['A'], ['AA'], ['ABC'], ['ACB']]
    EXPECTED_2 = [['A', 'AA'], ['ABC', 'ACB']]
    EXPECTED_3 = [['A', 'AA', 'ABC'], ['ACB']]
    EXPECTED_MAX = [['A', 'AA', 'ABC', 'ACB']]
    TICKER_NAME = 'symbols'
    OUT_NAME = 'symbol_groups'

    def test_max_1(self):
        self.assertEqual(self.EXPECTED_1, ChunkerComp(max_count=1, in_name=self.TICKER_NAME,
                         out_name=self.OUT_NAME).compute(**self.TICKERS)[self.OUT_NAME])

    def test_max_2(self):
        self.assertEqual(self.EXPECTED_2, ChunkerComp(max_count=2, in_name=self.TICKER_NAME,
                         out_name=self.OUT_NAME).compute(**self.TICKERS)[self.OUT_NAME])

    def test_max_3(self):
        self.assertEqual(self.EXPECTED_3, ChunkerComp(max_count=3, in_name=self.TICKER_NAME,
                         out_name=self.OUT_NAME).compute(**self.TICKERS)[self.OUT_NAME])

    def test_max_4(self):
        self.assertEqual(self.EXPECTED_MAX, ChunkerComp(max_count=4, in_name=self.TICKER_NAME,
                         out_name=self.OUT_NAME).compute(**self.TICKERS)[self.OUT_NAME])

    def test_max_100(self):
        self.assertEqual(self.EXPECTED_MAX, ChunkerComp(max_count=100, in_name=self.TICKER_NAME,
                         out_name=self.OUT_NAME).compute(**self.TICKERS)[self.OUT_NAME])


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
    def val_method(key, value):
        if value > 10:
            raise ValueError("'number' param's value too high")

    def test_custom_validation_exception(self):
        '''
        Asserts that if a parameter is wrong an ValueError is raised.
        '''
        with self.assertRaises(expected_exception=ValueError):
            ParamValidatorComp(validator_mapping={'number': self.val_method}).compute(**self.WRONG_NUMBER_PARAM)

    def test_match_validation_exception(self):
        '''
        Asserts that if a parameter is wrong an ValueError is raised by the match validation method.
        '''
        with self.assertRaises(expected_exception=ValueError):
            interval_match_validator = match_param_validation(
                possible_values=self.POSSIBLE_INTERVALS)
            ParamValidatorComp(validator_mapping={'interval': interval_match_validator}).compute(
                **self.WRONG_INTERVAL_PARAM)

    def test_dt_param_exception(self):
        dt_validator = datetime_param_validation(dt_format=self.DT_FORMAT, required=True)
        with self.assertRaises(expected_exception=ValueError):
            ParamValidatorComp(validator_mapping={'start': dt_validator}).compute(**self.WRONG_DT_PARAM_1)
            ParamValidatorComp(validator_mapping={'start': dt_validator}).compute(**self.WRONG_DT_PARAM_2)


class SubAdapterTest(unittest.TestCase):

    class CustomComponent(AdapterComponent):

        def compute(self, **kwargs):
            kwargs['A'] = 1
            return kwargs

    def setUp(self):
        self.component = SubAdapter(components=[
            SubAdapterTest.CustomComponent(),
        ], list_name='list_name', out_name='out_name')

    def test_missing_list_param(self):
        kwargs = {'nothing': 'really'}
        with self.assertRaises(expected_exception=ValueError):
            self.component.compute(**kwargs)

    def test_list_param_not_list(self):
        kwargs = {'list_name': 1}
        with self.assertRaises(expected_exception=ValueError):
            self.component.compute(**kwargs)

    def test_components_see_element(self):
        kwargs = {'list_name': [1, 2, 3]}
        # Check that the last element seen is in the out_name param
        self.assertEqual(
            self.component.compute(**kwargs)['out_name'], 3
        )

    def test_component_is_called(self):
        kwargs = {'list_name': [1, 2, 3]}
        out_kwargs = self.component.compute(**kwargs)
        self.assertEqual(
            out_kwargs['A'], 1
        )

# TODO: Request component tests
