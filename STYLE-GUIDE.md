# OTRI Python style guide

##### This file will be updated every time a new rule will pop our head.

## 1 Code style

### 1.1 Naming
We use both CamelCasing and snake_case:

- camel case
 - class

- snake case
 - variable
 - method/function

#### 1.1.1 Class naming
Class names start uppercase and should be not too short nouns that fully describe what the class is meant to do.

#### 1.1.2 Method naming
All methods names are lowercase and should start with a verb or some pronoun that implies an action. Name length should be as few different words as possible, sometimes it is possible to avoid using the verb if it's obvious and can be deceived from the class name (`DataImporter.import_from_file()` could just be `DataImporter.from_file()`).

#### 1.1.3 Variable naming
All lowercase

### 1.2 Formatting
Python is based on code formatting, use 4 spaces for indentation, leave one empty line before every method declaration.

### 1.3 Documentation
Documentation is made using three consecutive single quote marks (''') one indentation in from the declaration it's documenting.
The first line of comment begins right under the first quote mark and contains the description.

#### 1.3.1 Class documentation
Class documentation should contain a description of the class followed by the list of attributes of the class.

#### 1.3.2 Method documentation
Method documentation should contain a description of the method followed by the parameters it requires, the exceptions it raises and the possible return values.
Leave a space between the description and the other fields.

```Python
def method_name(self, a : str):
    '''
    Method description

    Parameters:
        ... # see Variable/Parameter documentation
    Raises: 
        ... # see Exception documentation
    Returns:
        ... # see Returns documentation
```

#### 1.3.3 Variable/Parameter documentation
Parameter documentation is a list of parameters followed by their expected type and on a new line, indented by 1, a description of what they should contain.

```Python
def method_name(self, a : str, d : dict):
    '''
    Method description

    Parameters:
        a : str
            Parameter description
        d : dict
            Parameter description
```

#### 1.3.4 Exception documentation

```Python
def method_name(self, a : str, d : dict):
    '''
    Method description

    Raises:
        NotImplementedException
            This method should only be called on child classes
    '''
```

```Python

def ClassName: 
    '''
    Description of the class

    Attributes:
        var : str
            Description of the variable
    '''

    def method(self, a : str):
    '''
    Description of the method
  
    Parameters:
        a : str
            Description of the requested parameter
    Raises:
        NotImplementedError
            Description of the conditions that could raise the exception
    Returns:
        Description of the returned values
      '''
    return a
```