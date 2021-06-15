import unittest
from typing import Callable

from otri.filtering.stream import CallbackQueue, LocalStream


class CallbackQueueTest(unittest.TestCase):

    def default_callback(self, data):
        self.DEFAULT_1_CALLED = True

    def default_callback_2(self, data):
        self.DEFAULT_2_CALLED = True

    def default_callback_3(self, data):
        self.out_value = data

    def setUp(self):
        self.DEFAULT_1_CALLED = False
        self.DEFAULT_2_CALLED = False
        self.out_value = None
        self.default_queue = CallbackQueue(callback=self.default_callback)

    def test_constructor_callback(self):
        self.default_queue.push("element")
        self.assertTrue(self.DEFAULT_1_CALLED)

    def test_multiple_callbacks(self):
        self.default_queue.add_callback(callback=self.default_callback_2)
        self.default_queue.push("element")
        self.assertTrue(self.DEFAULT_1_CALLED)
        self.assertTrue(self.DEFAULT_2_CALLED)

    def test_passing_data_push(self):
        '''Asserts that the method receives the right element'''
        self.default_queue.add_callback(callback=self.default_callback_3)
        self.default_queue.push("test_element")
        self.assertEqual(self.out_value, "test_element")

    def test_passing_data_push_all(self):
        '''Asserts that the methods are called for every single element in the push_all argument'''
        self.default_queue.add_callback(callback=self.default_callback_3)
        self.default_queue.push_all(elements=["test_element1", "test_element2"])
        self.assertEqual(self.out_value, "test_element2")

# Testing class extension

class LocalCallbackQueue(CallbackQueue, LocalStream):
    pass


class LocalCallbackQueueTest(unittest.TestCase):
    '''Tests that callback functionalities still work when mixed with other queue types.'''

    def default_callback(self, data):
        self.DEFAULT_1_CALLED = True

    def default_callback_2(self, data):
        self.DEFAULT_2_CALLED = True

    def default_callback_3(self, data):
        self.out_value = data

    def setUp(self):
        self.DEFAULT_1_CALLED = False
        self.DEFAULT_2_CALLED = False
        self.out_value = None
        self.default_queue = LocalCallbackQueue(callback=self.default_callback)

    def test_constructor_callback(self):
        self.default_queue.push("element")
        self.assertTrue(self.DEFAULT_1_CALLED)

    def test_multiple_callbacks(self):
        self.default_queue.add_callback(callback=self.default_callback_2)
        self.default_queue.push("element")
        self.assertTrue(self.DEFAULT_1_CALLED)
        self.assertTrue(self.DEFAULT_2_CALLED)

    def test_passing_data_push(self):
        '''Asserts that the method receives the right element'''
        self.default_queue.add_callback(callback=self.default_callback_3)
        self.default_queue.push("test_element")
        self.assertEqual(self.out_value, "test_element")

    def test_passing_data_push_all(self):
        '''Asserts that the methods are called for every single element in the push_all argument'''
        self.default_queue.add_callback(callback=self.default_callback_3)
        self.default_queue.push_all(elements=["test_element1", "test_element2"])
        self.assertEqual(self.out_value, "test_element2")
