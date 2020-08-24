from otri.validation.validators.cluster_validator import ClusterValidator
from otri.filtering.stream import Stream
from otri.validation.exceptions import ClusterWarning

from .. import find_error

from typing import Callable, Iterable
import unittest


cluster_example_data = (

    # Clusters of 1 (limit 0): all elements are clusters.
    (lambda data: find_error(data, ClusterWarning),
     [{"number": x} for x in range(100)],
     [True] * 100, "number", 0),

    # Clusters of 1: all elements are clusters even if equal.
    (lambda data: find_error(data, ClusterWarning),
     [{"number": x // 2} for x in range(100)],
     [True] * 100, "number", 0),

    # Clusters of 3 (limit 2).
    (lambda data: find_error(data, ClusterWarning),
     [{"number": x} for x in [1, 1, 2, 3, 3, 3, 5]],
     [False] * 3 + [True] * 3 + [False], "number", 2)
)


class ClusterValidatorTest(unittest.TestCase):

    def template(self, find: Callable, test_data: Iterable, expected: Iterable, key: str, limit: int):
        '''
        Parameters:

            find : Callable
                Function converting an output list into some evaluable result.

            test_data : Iterable
                The test data to put in the input Streams. Must be a list of input datasets.

            expected : Iterable
                The expected results after passing through `find`. Must be a list of expected outputs.
                Must be the same size as test_data.

            key : str
                Key to monitor in the Validator.

            limit : int
                Cluster limit int the Validator.
        '''
        if len(test_data) != len(expected):
            raise ValueError("Lengths must be the same.")

        self.filter = ClusterValidator("in", "out", key, limit)
        self.input = Stream(test_data, is_closed=True)
        self.output = Stream()
        self.state = dict()
        self.filter.setup([self.input], [self.output], self.state)

        while not self.filter._are_outputs_closed():
            self.filter.execute()

        # Check the output is correct, both length and values.
        self.assertListEqual(find(self.output), expected)

    def test_single_element_clusters(self):
        '''
        Check that if limit is 0 all different elements are a cluster.
        '''
        self.template(*cluster_example_data[0])

    def test_two_elements_clusters(self):
        '''
        Check that if limit is 0, two element clusters are found.
        '''
        self.template(*cluster_example_data[1])

    def test_simple_cluster(self):
        '''
        Find a simple cluster inside a Stream.
        '''
        self.template(*cluster_example_data[2])
