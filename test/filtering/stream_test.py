from otri.filtering.stream import Stream, StreamIter
import unittest

sample_initial_list = [1,2,3,4]

class StreamTest(unittest.TestCase):

    def setUp(self):
        self.default_steam = Stream(sample_initial_list)
    
    def test_iter_type(self):
        # Testing iter type is StreamIter
        self.assertTrue(isinstance(self.default_steam.__iter__(), StreamIter), "It's actually {}".format(type(self.default_steam.__iter__)))

    def test_iter_next_element(self):
        # Testing that next pops the right element
        self.assertEqual(1, next(self.default_steam.__iter__()))

    def test_iter_next_leftover(self):
        # Testing that next actually removes the element from the stream
        next(self.default_steam.__iter__())
        self.assertEqual([2,3,4], self.default_steam)