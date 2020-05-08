from typing import Sequence, Callable, Mapping, List, Any


class AtomValidatorInterface():
    '''
    Interface defining basic behaviour of a standard AtomValidator Object.
    '''

    def __init__(self):
        super().__init__()

    def get_checks(self) -> Sequence[Callable]:
        '''
        Returns:
            The Sequence of checks this Validator would apply on a given list
            of atoms.
        '''
        pass

    def add_checks(self, *args: Sequence[Callable]):
        '''
        Add one or more checks to use when validating.

        Parameters:
            *args : Sequence[Callable]
                A list of functions or lambdas that expect the list of
                atoms as a parameter.
        '''
        pass

    def remove_checks(self, *args: Sequence[Callable]):
        '''
        Remove one or more checks from the ones to apply
        when validating.

        Parameters:
            *args : Sequence[Callable]
                A list of functions or lambdas. They will not
                be removed if not currently present.
        '''
        pass

    def validate(self, atoms: Sequence) -> Mapping[Callable, Any]:
        '''
        Applies all previously provided checks to the given list.

        Parameters:
            atoms : Sequence[Mapping]
                The sequence of atoms to validate, will be passed
                to all previously given checks.
        Returns:
            A Mapping of each check to its result.
        '''
        pass


class BaseValidator(AtomValidatorInterface):
    '''
    A simple class performing the given checks, with no other particular logic.
    '''

    def __init__(self):
        super().__init__()
        self.__checks = list()

    def get_checks(self) -> List[Callable]:
        '''
        Returns:
            The list of checks this Validator would apply on a given list
            of atoms.
        '''
        return self.__checks

    def add_checks(self, *args: Sequence[Callable]):
        '''
        Add one or more checks to use when validating.

        Parameters:
            *args : Sequence[Callable]
                A list of functions or lambdas that expect the list of
                atoms as a parameter.
        '''
        self.__checks.extend(args)

    def remove_checks(self, *args: Sequence[Callable]):
        '''
        Remove one or more checks from the ones to apply
        when validating.

        Parameters:
            *args : Sequence[Callable]
                A list of functions or lambdas. They will not
                be removed if not currently present.
        '''
        for x in [a for a in args if a in self.__checks]:
            self.__checks.remove(x)

    def validate(self, atoms: Sequence) -> Mapping[Callable, Any]:
        '''
        Applies all previously provided checks to the given list.

        Parameters:
            atoms : Sequence
                The sequence of atoms to validate, will be passed
                to all previously given checks.
        Returns:
            A Mapping of each check to its result.
        '''
        return {check : check(atoms) for check in self.__checks}