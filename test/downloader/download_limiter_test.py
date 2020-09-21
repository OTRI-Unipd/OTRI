import unittest
from otri.downloader import DefaultRequestsLimiter
from datetime import timedelta, datetime


class DefaultRequestsLimiterTest(unittest.TestCase):

    def setUp(self):
        self.limiter = DefaultRequestsLimiter(10, timedelta(seconds=1))

    def test_on_request_resets(self):
        self.limiter._on_request()
        self.assertEqual(1, self.limiter.request_counter)
        self.assertLessEqual(datetime.utcnow(), self.limiter.next_reset)

    def test_wait_time_not_zero(self):
        for _ in range(10):
            self.limiter._on_request()
        self.assertLessEqual(0, self.limiter.waiting_time())
