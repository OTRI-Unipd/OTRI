from otri.filtering.filter import Filter, ParallelFilter, Stream
from unittest.mock import MagicMock
from parameterized import parameterized_class
import unittest


class FilterTest(unittest.TestCase):

    def setUp(self):
        self.s_A = Stream([1, 2, 3])
        self.s_B = Stream([3, 4, 5])
        self.s_D = Stream()
        self.s_E = Stream()
        self.s_F = Stream()
        self.state = dict()
        self.f = Filter(
            inputs=["A", "B"],
            outputs=["D", "E", "F"],
            input_count=2,
            output_count=3
        )
        self.f.setup([self.s_A, self.s_B], [self.s_D, self.s_E, self.s_F], self.state)

    def test_filter_input_number_correct(self):
        self.assertEqual(2, len(self.f.get_input_names()))

    def test_filter_input_stream_names_equals(self):
        self.assertEqual(["A", "B"], self.f.get_input_names())

    def test_filter_output_number_correct(self):
        self.assertEqual(3, len(self.f.get_output_names()))

    def test_filter_output_stream_names_equals(self):
        self.assertEqual(["D", "E", "F"], self.f.get_output_names())

    def test_get_in_streams(self):
        self.assertEqual(self.s_A, self.f._get_input(0))
        self.assertEqual(self.s_B, self.f._get_input(1))

    def test_get_out_streams(self):
        self.assertEqual(self.s_D, self.f._get_output(0))
        self.assertEqual(self.s_E, self.f._get_output(1))
        self.assertEqual(self.s_F, self.f._get_output(2))

    def test_get_in_iters(self):
        self.assertEqual(iter(self.s_A), self.f._get_in_iter(0))
        self.assertEqual(iter(self.s_B), self.f._get_in_iter(1))

    def test_get_out_iters(self):
        self.assertEqual(iter(self.s_D), self.f._get_out_iter(0))
        self.assertEqual(iter(self.s_E), self.f._get_out_iter(1))
        self.assertEqual(iter(self.s_F), self.f._get_out_iter(2))

    def test_pop_data(self):
        self.assertEqual(1, self.f._pop_data(0))
        self.assertEqual(3, self.f._pop_data(1))

    def test_push_data(self):
        self.f._push_data(5, 0)
        self.assertEqual(5, self.s_D.__iter__().__next__())

    def test_execute_outputs_closed(self):
        self.s_D.close()
        self.s_E.close()
        self.s_F.close()
        self.f._on_outputs_closed = MagicMock()
        self.f.execute()
        self.assertTrue(self.f._on_outputs_closed.called)

    def test_execute_on_data(self):
        self.f._on_data = MagicMock()
        self.f.execute()
        self.assertTrue(self.f._on_data.called)

    def test_execute_input_empty(self):
        self.s_A.clear()
        self.s_B.clear()
        self.f._on_inputs_empty = MagicMock()
        self.f.execute()
        self.assertTrue(self.f._on_inputs_empty.called)

    def test_execute_input_closed(self):
        self.s_A.clear()
        self.s_B.clear()
        self.s_A.close()
        self.s_B.close()
        self.f._on_inputs_closed = MagicMock()
        self.f.execute()
        self.assertTrue(self.f._on_inputs_closed.called)

    def test_default_on_inputs_closed_closes_outputs(self):
        self.s_A.clear()
        self.s_B.clear()
        self.s_A.close()
        self.s_B.close()
        self.f.execute()
        self.assertTrue(self.f._get_output(0).is_closed())
        self.assertTrue(self.f._get_output(1).is_closed())
        self.assertTrue(self.f._get_output(2).is_closed())


class ParallelFilterTest(unittest.TestCase):

    def setUp(self):
        self.s_A = Stream([1, 2, 3])
        self.s_B = Stream([3, 4, 5, 6])
        self.s_D = Stream()
        self.s_E = Stream()
        self.state = dict()
        self.f = ParallelFilter(
            inputs=["A", "B"],
            outputs=["D", "E"]
        )
        self.f.setup([self.s_A, self.s_B], [self.s_D, self.s_E], self.state)

    def test_filter_input_number_correct(self):
        self.assertEqual(2, len(self.f.get_input_names()))

    def test_filter_input_stream_names_equals(self):
        self.assertEqual(["A", "B"], self.f.get_input_names())

    def test_filter_output_number_correct(self):
        self.assertEqual(2, len(self.f.get_output_names()))

    def test_filter_output_stream_names_equals(self):
        self.assertEqual(["D", "E"], self.f.get_output_names())

    def test_get_in_streams(self):
        self.assertEqual(self.s_A, self.f._get_input(0))
        self.assertEqual(self.s_B, self.f._get_input(1))

    def test_get_out_streams(self):
        self.assertEqual(self.s_D, self.f._get_output(0))
        self.assertEqual(self.s_E, self.f._get_output(1))

    def test_get_in_iters(self):
        self.assertEqual(iter(self.s_A), self.f._get_in_iter(0))
        self.assertEqual(iter(self.s_B), self.f._get_in_iter(1))

    def test_get_out_iters(self):
        self.assertEqual(iter(self.s_D), self.f._get_out_iter(0))
        self.assertEqual(iter(self.s_E), self.f._get_out_iter(1))

    def test_pop_data(self):
        self.assertEqual(1, self.f._pop_data(0))
        self.assertEqual(3, self.f._pop_data(1))

    def test_push_data(self):
        self.f._push_data(5, 0)
        self.assertEqual(5, self.s_D.__iter__().__next__())

    def test_execute_outputs_closed(self):
        self.s_D.close()
        self.s_E.close()
        self.f._on_outputs_closed = MagicMock()
        self.f.execute()
        self.assertTrue(self.f._on_outputs_closed.called)

    def test_execute_on_data(self):
        self.f._on_data = MagicMock()
        self.f.execute()
        self.assertTrue(self.f._on_data.called)

    def test_execute_input_empty(self):
        self.s_A.clear()
        self.s_B.clear()
        self.f._on_inputs_empty = MagicMock()
        self.f.execute()
        self.assertTrue(self.f._on_inputs_empty.called)

    def test_execute_input_closed(self):
        self.s_A.clear()
        self.s_B.clear()
        self.s_A.close()
        self.s_B.close()
        self.f._on_inputs_closed = MagicMock()
        self.f.execute()
        self.assertTrue(self.f._on_inputs_closed.called)

    def test_default_on_inputs_closed_closes_outputs(self):
        self.s_A.clear()
        self.s_B.clear()
        self.s_A.close()
        self.s_B.close()
        self.f.execute()
        self.assertTrue(self.f._get_output(0).is_closed())
        self.assertTrue(self.f._get_output(1).is_closed())

    def test_parallelism(self):
        self.f._on_data = lambda data, index: self.assertTrue(len(data) == 2 and len(index) == 2)
        self.f.execute()

    def test_uneven_inputs(self):
        # Mock the _on_data method
        self.f._on_data = MagicMock()
        self.f.execute()
        self.f.execute()
        self.f.execute()
        # Assert the inputs are uneven.
        self.assertListEqual(self.f._get_input(0), [])
        self.assertListEqual(self.f._get_input(1), [6])
        # Assert the last item is passed with the correct index.
        self.f._on_data = lambda data, index: self.assertTrue(data == [6] and index == [1])
        self.f.execute()
