from otri.filtering.stream import Stream, StreamIter
import unittest

sample_initial_list = [1, 2, 3, 4]


class StreamTest(unittest.TestCase):

    def setUp(self):
        self.default_stream = Stream(sample_initial_list)

    def test_iter_type(self):
        # Testing iter type is StreamIter
        self.assertTrue(isinstance(self.default_stream.__iter__(), StreamIter))

    def test_iter_next_element(self):
        # Testing that next pops the right element
        self.assertEqual(1, next(self.default_stream.__iter__()))

    def test_iter_next_leftover(self):
        # Testing that next actually removes the element from the stream
        next(self.default_stream.__iter__())
        self.assertEqual([2, 3, 4], self.default_stream)

    def test_stream_append(self):
        self.default_stream.append(5)
        self.assertEqual([1, 2, 3, 4, 5], self.default_stream)

    def test_stream_insert(self):
        self.default_stream.insert(0, 5)
        self.assertEqual([5, 1, 2, 3, 4], self.default_stream)

    def test_closed_stream_append(self):
        self.default_stream.close()
        self.assertRaises(RuntimeError, self.default_stream.append, 5)

    def test_closed_stream_insert(self):
        self.default_stream.close()
        self.assertRaises(RuntimeError, self.default_stream.insert, 0, 5)

    def test_streamIter_stream_has_next(self):
        self.assertTrue(self.default_stream.__iter__().has_next())

    def test_streamIter_empty_stream_has_next(self):
        stream = Stream()
        self.assertFalse(stream.__iter__().has_next())

    def test_streamIter_empty_stream_next(self):
        stream = Stream()
        self.assertRaises(StopIteration, next, iter(stream))

    def test_streamIter_closed_stream_next_ok(self):
        self.default_stream.close()
        self.assertEqual(1,next(self.default_stream.__iter__()))

    def test_stream_is_open(self):
        self.assertFalse(self.default_stream.is_closed())
    
    def test_closed_stream_close_again(self):
        self.default_stream.close()
        self.assertRaises(RuntimeError, self.default_stream.close)