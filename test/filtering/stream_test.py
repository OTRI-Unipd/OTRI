from otri.filtering.queue import LocalQueue, ClosedQueueError
import unittest

sample_initial_list = [1, 2, 3, 4]


class QueueTest(unittest.TestCase):

    def setUp(self):
        self.default_queue = LocalQueue(sample_initial_list)

    def test_queue_append(self):
        self.default_queue.append(5)
        self.assertEqual(LocalQueue([1, 2, 3, 4, 5]), self.default_queue)

    def test_closed_queue_append(self):
        self.default_queue.close()
        self.assertRaises(ClosedQueueError, self.default_queue.push, 5)

    def test_queueIter_queue_has_next(self):
        self.assertTrue(self.default_queue.has_next())

    def test_queue_is_open(self):
        self.assertFalse(self.default_queue.is_closed())
    
    def test_closed_queue_close_again(self):
        self.default_queue.close()
        self.assertRaises(ClosedQueueError, self.default_queue.close)