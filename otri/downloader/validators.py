'''
Contains methods to build validation functions for ParamValidationComp validation mapping.
'''
from datetime import datetime
from typing import Callable, Any, Iterable


def match_param_validation(possible_values: Iterable[Any], required: bool = True) -> Callable:
    '''
    Generates a validation method that checks if the parameter's value the possible values for the key.
    The method raises exception when the parameter's value is NOT between the possible ones.

    Parameters:
        possible_values : Iterable[Any]
            Collection of possible values for the parameter.
        required : bool
            Whether the parameter is required or not.
    Returns:
        A callable validation method.
    '''
    def validator(key, value):
        if value is None:
            if required:
                raise ValueError(f"Parameter '{key}' cannot be None")
            return
        if value not in possible_values:
            raise ValueError(f"{value} not a possible value for '{key}', possible values: {possible_values}")
    return validator


def datetime_param_validation(dt_format: str, required: bool = True) -> Callable:
    '''
    Generates a validation method that checks if the parameter's value is a datetime with the given format.
    The method raises exception when the parameter's value is NOT a datetime or in the given format.

    Parameters:
        dt_format : str
            strptime format to parse the parameter's value.
        required : bool
            Whether the parameter is required or not.
    Returns:
        A callable validation method.
    '''
    def validator(key, value):
        if value is None:
            if required:
                raise ValueError(f"Parameter '{key}' cannot be None")
            return
        if not isinstance(value, str):
            raise ValueError(f"Parameter '{key}' is not a string")
        try:
            datetime.strptime(value, dt_format)
        except ValueError:
            raise ValueError(f"Parameter '{key}' with value {value} does not match datetime format {dt_format}")
    return validator

# TODO: another default validation could be range check (value in range [min, max])
# TODO: another default validation is just checking that a parameter is given
# TODO: another default validation is regex.
