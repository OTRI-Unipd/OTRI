import unittest

from otri.filtering.stream import ClosedStreamError, LocalStream

sample_initial_list = [1, 2, 3, 4]


class StreamTest(unittest.TestCase):

    def setUp(self):
        self.default_stream = LocalStream(sample_initial_list)

    def test_stream_append(self):
        self.default_stream.append(5)
        self.assertEqual(LocalStream([1, 2, 3, 4, 5]), self.default_stream)

    def test_closed_stream_append(self):
        self.default_stream.close()
        self.assertRaises(ClosedStreamError, self.default_stream.push, 5)

    def test_streamIter_stream_has_next(self):
        self.assertTrue(self.default_stream.has_next())

    def test_stream_is_open(self):
        self.assertFalse(self.default_stream.is_closed())

    def test_closed_stream_close_again(self):
        self.default_stream.close()
        self.assertRaises(ClosedStreamError, self.default_stream.close)
