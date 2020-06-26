from otri.filtering.filter import Filter, Stream
import unittest

s_1 = Stream([1, 2, 3])
s_2 = Stream([3, 4, 5])


class FilterTest(unittest.TestCase):

    def setUp(self):
        self.f = Filter([s_1, s_2], 2, 4)

    def test_filter_input_number_correct(self):
        self.assertEqual(2, len(self.f.get_input_streams()))
    
    def test_filter_input_streams_equals(self):
        self.assertEqual([s_1,s_2], self.f.get_input_streams())
    
    def test_filter_get_input_stream(self):
        self.assertEqual(s_1, self.f.get_input_stream(0))
        self.assertEqual(s_2, self.f.get_input_stream(1))

    def test_filter_output_number_correct(self):
        self.assertEqual(4, len(self.f.get_output_streams()))

    def test_filter_all_outputs_closed_is_finished(self):
        for o in self.f.get_output_streams():
            o.close()
        self.assertTrue(self.f.is_finished()) 

    def test_filter_some_outputs_closed_is_not_finished(self):
        for i in range(1,len(self.f.get_output_streams())):
            self.f.get_output_stream(i).close()
        self.assertFalse(self.f.is_finished())

    def test_filter_execute_raise_exception(self):
        self.assertRaises(NotImplementedError, self.f.execute)
