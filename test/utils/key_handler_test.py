import otri.utils.key_handler as kh
import unittest

reverse = lambda s : s[::-1] if isinstance(s, str) else s

simple_dict = {"Hello" : "Ciao", "How" : "Come", "Are you?" : "Stai?"}
simple_list = ["Io", "Sono", "Groot"]

nested_dict = {"Hello There":{"General" : "Kenobi"}}
list_of_dicts = [{"What" : "shall we do"}, {"with" : "the drunken sailor?"}]

something_complex = {
    "atoms":[
        {"time":"early", "money": "a few"},
        {"time":"noon", "money":"meh"},
        {"time": "late", "money":"stonks"}
    ],
    "meta":{"symbol":"FFVII"}
}

class ApplyDeepTest(unittest.TestCase):
    def test_simple_dict_keys_change(self):
        # Testing key change on a single level dict
        expected_keys = set(reverse(k) for k in simple_dict.keys())
        self.assertEqual(set(kh.apply_deep(simple_dict, reverse).keys()), expected_keys)

    def test_simple_dict_same_values(self):
        # Testing the values are the same on a single level dict
        expected_values = list(simple_dict.values())
        self.assertCountEqual(kh.apply_deep(simple_dict, reverse).values(), expected_values)

    def test_simple_list_does_nothing(self):
        # Testing a list with no dictionaries remains unchanged
        self.assertCountEqual(kh.apply_deep(simple_list, reverse), simple_list)

    def test_dict_in_dict_keys_change(self):
        # Testing a nested dict key change
        expected = {"erehT olleH":{"lareneG" : "Kenobi"}}
        self.assertCountEqual(kh.apply_deep(nested_dict, reverse).keys(), expected.keys())

    def test_dict_in_dict_same_values(self):
        # Testing a nested dict's values
        expected = {"erehT olleH":{"lareneG" : "Kenobi"}}
        self.assertCountEqual(kh.apply_deep(nested_dict, reverse).values(), expected.values())

    def test_list_of_dict_keys_change(self):
        # Testing a list of dicts' keys change
        expected = [{"tahW" : "shall we do"}, {"htiw" : "the drunken sailor?"}]
        actual = kh.apply_deep(list_of_dicts, reverse)
        for i, d in enumerate(actual):
            self.assertCountEqual(d.keys(), expected[i].keys())

    def test_list_of_dict_same_values(self):
        # Testing a list of dicts' keys stay unchanged
        expected = [{"tahW" : "shall we do"}, {"htiw" : "the drunken sailor?"}]
        actual = kh.apply_deep(list_of_dicts, reverse)
        for i,d in enumerate(actual):
            self.assertCountEqual(d.values(), expected[i].values())

    def test_more_complex(self):
        # Testing on a more complex dict
        expected = {
            "smota":[
                {"emit":"early", "yenom": "a few"},
                {"emit":"noon", "yenom":"meh"},
                {"emit": "late", "yenom":"stonks"}
            ],
            "atem":{"lobmys":"FFVII"}
        }
        self.assertEqual(kh.apply_deep(something_complex, reverse), expected)

dict_to_replace = {
            "market": "MGP",
            "totale_acquisti": "24324,154",
            "datetime": "2020-04-28 01:00:00.000"
        }

class ReplaceDeepTest(unittest.TestCase):
    def test_simple_dict_keys_change(self):
        # Testing key change on a single level dict
        expected_dict = {
            "market": "MGP",
            "totale_paolo": "24324,154",
            "datetime": "2020-04-28 01:00:00.000"
        }
        self.assertEqual(kh.replace_deep(dict_to_replace, {"acquisti" : "paolo"}), expected_dict)