from otri.filtering.filter import Filter, Stream
from unittest.mock import MagicMock
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
        self.assertEqual(2, len(self.f.input_names))

    def test_filter_input_stream_names_equals(self):
        self.assertEqual(["A", "B"], self.f.input_names)

    def test_filter_output_number_correct(self):
        self.assertEqual(3, len(self.f.output_names))

    def test_filter_output_stream_names_equals(self):
        self.assertEqual(["D", "E", "F"], self.f.output_names)

    def test_get_in_streams(self):
        self.assertEqual(self.s_A, self.f._get_inputs()[0])
        self.assertEqual(self.s_B, self.f._get_inputs()[1])

    def test_get_out_streams(self):
        outputs = self.f._get_outputs()
        self.assertEqual([self.s_D, self.s_E, self.s_F], outputs)

    def test_push_data(self):
        self.f._push_data(5, 0)
        self.assertEqual(5, self.s_D.pop())

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
        self.assertTrue( self.f._on_data.called)

    def test_execute_input_empty(self):
        self.s_A.clear()
        self.s_B.clear()
        self.f._on_inputs_empty = MagicMock()
        self.f.execute()
        self.assertTrue( self.f._on_inputs_empty.called)

    def test_execute_input_closed(self):
        self.s_A.clear()
        self.s_B.clear()
        self.s_A.close()
        self.s_B.close()
        self.f._on_inputs_closed = MagicMock()
        self.f.execute()
        self.assertTrue( self.f._on_inputs_closed.called)

    def test_default_on_inputs_closed_closes_outputs(self):
        self.s_A.clear()
        self.s_B.clear()
        self.s_A.close()
        self.s_B.close()
        self.f.execute()
        self.assertTrue( self.f._get_outputs()[0].is_closed())
        self.assertTrue( self.f._get_outputs()[1].is_closed())
        self.assertTrue( self.f._get_outputs()[2].is_closed())