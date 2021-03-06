from typing import *
import re

LOWER_ERROR = "Only dictionaries and lists can be modified by this method."


def apply_deep(data: Union[Mapping, List], fun: Callable) -> Union[dict, list]:
    '''
    Applies fun to all keys in data.
    The method is recursive and applies as deep as possible in the dictionary nest. 

    Parameters:
        data : Mapping or List
            Data to modify, must be either a dictionary or a list of dictionaries.
        fun : function | lambda
            Function to apply to each key, must take the key as its single parameter.
    Returns:
        A copy of the dict or list with the modified keys, with all nested dicts and list
        receiving the same treatment. It will return the original
        object (not a copy) if no operation could be applied, for example when:
        - data is not a list or dict
        - data is a list of non dict items
        - data is not a list that contains dicts at any nesting level
        ...
    '''
    if isinstance(data, Mapping):
        return __apply_deep_dict(data, fun)
    if isinstance(data, List):
        return __apply_deep_list(data, fun)
    return data


def __apply_deep_dict(data: Mapping, fun: Callable) -> dict:
    '''
    Applies fun to all keys in a dictionary and all nested items.

    Parameters:
        data : dict
            Data to modify, must be a dictionary.
        fun : function | lambda
            Function to apply to each key, must take the key as its single parameter.
    Returns:
        A copy of the dict with the renamed keys, where all values have been replaced by copies of
        their original if apply_deep(value, fun) was appliable.
    '''
    new_data = dict()
    for key, value in data.items():
        new_key = fun(key)
        new_data[new_key] = apply_deep(value, fun)
    return new_data


def __apply_deep_list(data: List, fun : Callable) -> list:
    '''
    Applies fun to all keys in each item of the list, if appliable.

    Parameters:
        data : List
            Data to modify, should be a list, but can be a tuple.
        fun : function | lambda
            Function to apply to each key, must take the key as its single parameter.
    Returns:
        A copy of the list, where each item got its keys modified through apply_deep(item, fun) if appliable.
    '''
    return [apply_deep(item, fun) for item in data]


def lower_all_keys_deep(data : Union[Mapping, List]) -> Union[dict, list]:
    '''
    Renames all the keys in a dict object to be lower case.
    The method is recursive and applies as deep as possible in the dict nest. 

    Parameters:
        data : dict | list
            Data to modify, must be either a dictionary or a list of dictionaries.
            Should work with any dictionary. In any case, only string keys will be modified.
    Returns:
        A copy of the dict or list with the renamed keys, with all nested dicts and list
        receiving the same treatment. It will return the original
        object (not a copy) if no operation could be applied. See apply_deep(data, fun) for details.
        ...
    '''
    return apply_deep(data, lambda s: s.lower() if isinstance(s, str) else s)


def rename_deep(data : Union[Mapping, List], aliases: Mapping) -> Union[dict, list]:
    '''
    Renames the keys in the dict object based on the aliases in dict.
    The method is recursive and applies as deep as possible in the dict nest.
    es. data = {"key" : "value"}, aliases {"key", "one"}
    data becomes {"one" : "value"}

    Parameters:
        data : dict | list
            Data to modify, must be either a dictionary or a list of dictionaries.
            Should work with any dictionary.
        aliases : dict
            Dictionary containing the aliases for the keys. For each item the key must be
            the original key and the value the new key. Keys of any type will be modified
            as long as they are a key in aliases.
    Returns:
        A copy of the dict or list with the renamed keys, with all nested dicts and list
        receiving the same treatment. It will return the original
        object (not a copy) if no operation could be applied. See apply_deep(data, fun) for details.
    '''
    return apply_deep(data, lambda x: aliases[x] if x in aliases.keys() else x)


def replace_deep(data : Union[Mapping, List], regexes: Mapping) -> Union[dict, list]:
    '''
    Renames the keys in a dictionary replacing each given regex with the given alias.
    The method is recursive and applies as deep as possible in the dict nest.
    es. data = {"key_ciao" : "value"}, aliases {"ciao", "hi"}
    data becomes {"key_hi" : "value"}

    Parameters:
        data : dict | list
            Data to modify, must be either a dictionary or a list of dictionaries.
            Should work with any dictionary.
        aliases : dict
            Dictionary containing the aliases for the keys. For each item the key must be
            the regex to replace and the value what to replace it with.
            Only string keys are modified.
    Returns:
        A copy of the dict or list with the renamed keys, with all nested dicts and lists
        receiving the same treatment. It will return the original object (not a copy)
        if no operation could be applied. See apply_deep(data, fun) for details.
    '''
    def replace_regex(string, regexes=regexes):
        for r, s in regexes.items():
            string = re.sub(r, s, string)
        return string
    return apply_deep(data, lambda x: replace_regex(x) if isinstance(x, str) else x)
