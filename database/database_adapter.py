'''
Classes:
DatabaseAdapter -- Abstract class
'''
class DatabaseAdapter:
    '''
        Abstract class used to access with the same methods independently from the kind of database used.
    '''
    def __init__(self):
        print("Ciao")