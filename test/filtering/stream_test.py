from otri.filtering.stream import Stream, ClosedStreamError
import unittest

sample_initial_list = [1, 2, 3, 4]


class StreamTest(unittest.TestCase):

    def setUp(self):
        self.default_stream = Stream(sample_initial_list)

    def test_stream_append(self):
        self.default_stream.append(5)
        self.assertEqual([1, 2, 3, 4, 5], self.default_stream)

    def test_closed_stream_append(self):
        self.default_stream.close()
        self.assertRaises(ClosedStreamError, self.default_stream.append, 5)

    def test_closed_stream_insert(self):
        self.default_stream.close()
        self.assertRaises(CLosedStreamError, self.default_stream.insert, 0, 5)

    def test_streamIter_stream_has_next(self):
        self.assertTrue(self.default_stream.__iter__().has_next())

    def test_stream_is_open(self):
        self.assertFalse(self.default_stream.is_closed())
    
    def test_closed_stream_close_again(self):
        self.default_stream.close()
        self.assertRaises(CLosedStreamError, self.default_stream.close)