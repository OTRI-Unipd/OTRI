from otri.filtering.stream import Stream, StreamIter
import unittest

sample_initial_list = [1, 2, 3, 4]


class StreamTest(unittest.TestCase):

    def setUp(self):
        self.default_stream = Stream(sample_initial_list)

    def test_iter_type(self):
        # Testing iter type is StreamIter
        self.assertTrue(isinstance(self.default_stream.__iter__(
        ), StreamIter), "It's actually {}".format(type(self.default_stream.__iter__)))

    def test_iter_next_element(self):
        # Testing that next pops the right element
        self.assertEqual(1, next(self.default_stream.__iter__()))

    def test_iter_next_leftover(self):
        # Testing that next actually removes the element from the stream
        next(self.default_stream.__iter__())
        self.assertEqual([2, 3, 4], self.default_stream)
    
    def test_stream_append(self):
        self.default_stream.append(5)
        self.assertEqual([1,2,3,4,5],self.default_stream)

    def test_stream_insert(self):
        self.default_stream.insert(0,5)
        self.assertEqual([5,1,2,3,4],self.default_stream)
        
    def test_closed_stream_append(self):
        self.default_stream.close()
        self.assertRaises(RuntimeError,self.default_stream.append,5)

    def test_closed_stream_insert(self):
        self.default_stream.close()
        self.assertRaises(RuntimeError,self.default_stream.insert,0,5)