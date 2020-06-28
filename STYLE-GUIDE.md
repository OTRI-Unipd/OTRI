# OTRI Python style guide

_This file will be updated every time a new rule will pop our head._

If you want to skip to an example [you can see it here](##Code-example)

## 1 Code style
This section is about mere text styling, it's the first one because having a consistent style is probably the most important thing to speed up code reviews and have easily refactorable code.

### 1.1 Naming
We use both CamelCasing and snake_case:

- Camel case
  - class names

- Snake case
  - variables, parameters
  - method names, function names

### 1.1.1 Class naming
Class names start uppercase and should be not too short nouns that fully describe what the class is meant to do.

### 1.1.2 Method naming
All methods names are lowercase and should start with a verb or some pronoun that implies an action. Name length should be as few different words as possible, sometimes it is possible to avoid using the verb if it's obvious and can be deceived from the class name (`DataImporter.import_from_file()` could just be `DataImporter.from_file()`).

When calling a method and ordered parameters association with requested parameters is not clear (you can't clearly tell what kind of parameters were requested) you should define the parameter's name they're referred to.

```Python
my_method("John", surname="Doe", age=97)
```

#### 1.1.2.1 Test methods naming
Test methods in unittest-extending classes should start with the `test_` prefix.
Method name should state clearly what kind of test will be performed and what state will be used (parameter's combination or outside classes state).

```Python
 def test_one_is_added(self):
        """ Testing the actual passed method is added to the list """
        def foo(x): return True
        valid = BaseValidator()
        valid.add_checks(foo)
        self.assertCountEqual(valid.get_checks(), [foo])
```

### 1.1.3 Variable naming
If it's a method or class variable it should always be lowecased, if it's an outside-class constant it can be all uppercase; in both cases they must be snake_cased.

## 1.2 Formatting
Python is based on code formatting, use 4 spaces for indentation, leave one empty line before every method declaration.

### 1.2.1 Symbols and Srings
- Strings should be declared within double quotes (")
- Documentation should use three consecutive single quotes (')

### 1.2.2 Spaces
- When listing things, always keep the comma attached to the left word and leave a space right after the comma `my_method(always, leave, a, single, space)`
- No spaces between parameters' names and their assigned value when calling a method `my_method(one="one", two="two")`

## 1.3 Documentation
Documentation is made using three consecutive single quote marks (''') one indentation in from the declaration it's documenting.
The first line of comment begins right under the first quote mark and contains the description.

### 1.3.1 Module documentation
If the file does not contain a class you should define documentation from the first line of the file unindented. You should also specify an `__author__`, `__all__` and `__version__` variables containing details.

### 1.3.2 Class documentation
Class documentation should contain a description of the class followed by the list of attributes of the class.

### 1.3.3 Method documentation
Method documentation should contain a description of the method followed by the parameters it requires, the exceptions it raises and the possible return values.
Leave a space between the description and the other fields.

```Python
def method_name(self, a : str) -> str:
    '''
    Method description

    Parameters:
        ... # see Variable/Parameter documentation
    Raises: 
        ... # see Exception documentation
    Returns:
        ... # see Returns documentation
```

### 1.3.4 Variable/Parameter documentation
Parameter documentation is a list of parameters followed by their expected type and on a new line, indented by 1, a description of what they should contain.

```Python
def method_name(self, a : str, d : dict) -> str:
    '''
    Method description

    Parameters:
        a : str
            Parameter description
        d : dict
            Parameter description
```

When possible try using `python.typing` types such as `Sequence`, `Collection`, `Mapping` instead of `dict` or `list`.

### 1.3.5 Exception documentation
You should clearly list the specific combination of parameters or state that could cause such exception.

```Python
def method_name(self, filename : str, d : dict):
    '''
    Method description

    Raises:
        FileNotFoundException
            Thrown when the given filename is not found.
    '''
```

## 1.4 Exceptions
_For exception raise documentation see_ [Exception documentation](###1.3.5-Exception-documentation)


#### 1.4.1 Try-except-finally
When writing an exception catching code you should leave an empty line between the last line of the above code block and the new keyword code block:

```Python
try:
    connection = psycopg2.connect(...)
    cursor = connection.cursor()

except(Exception, psycopg2.Error) as error:
    print("Connection threw an error: {}".format(error))
    return
```

DO NOT:
1. Catch/except an exception and do nothing about it unless it's a top level script (application scripts), document it and let it be thrown to the upper levels that will handle it.

#### 1.4.2 Raising an exception
Limit exception raising to cases where you cannot return any kind of sentinel value like `None`. Try being as explicative as you can in the exception error text, give as many insight details as possible so that debugging can be easier. Avoid at almost all cost the definition of a new type of exception, use the python default ones as much as you can.
Error text should start lowercased as it'll be probably concatenated with other text like shown above.

```Python
try:
    open(...)
except FileNotFoundError:
   return None or -1
```

```Python
raise ValueError("invalid interval {} for given date {}".format(interval, date))
```

### 1.5 Printing / debugging
Avoid printing anything in the console unless requested, if needed raise an exception.

## Code example

```Python

def ClassName: 
    '''
    Description of the class

    Attributes:
        var : str
            Description of the variable
    '''

    def method(self, a : str) -> str:
    '''
    Description of the method
  
    Parameters:
        a : str
            Description of the requested parameter
    Returns:
        Description of the returned values
      '''
    return a
```
