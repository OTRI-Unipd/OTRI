from otri.filtering.filter import Filter, Stream
import unittest

s_1 = Stream([1, 2, 3])
s_2 = Stream([3, 4, 5])

class FilterTest(unittest.TestCase):

    def setUp(self):
        self.f = Filter(
            inputs=["A","B"],
            outputs=["D","E","F"],
            input_count=2,
            output_count=3
        )

    def test_filter_input_number_correct(self):
        self.assertEqual(2, len(self.f.get_inputs()))
    
    def test_filter_input_streams_equals(self):
        self.assertEqual(["A","B"], self.f.get_inputs())

    def test_filter_output_number_correct(self):
        self.assertEqual(3, len(self.f.get_outputs()))

    def test_filter_execute_raise_exception(self):
        self.assertRaises(NotImplementedError, self.f.execute)

    def test_filter_setup_raise_exception(self):
        self.assertRaises(NotImplementedError, self.f.setup, [],[], [])