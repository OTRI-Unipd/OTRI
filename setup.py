from setuptools import setup

setup(
   name='otri',
   version='0.1',
   description='OTRI Python Module',
   author='UNIPD',
   author_email='',
   packages=['otri'],  #same as name
   install_requires=['psycopg2','yfinance', 'alpha-vantage'], #external packages as dependencies
   # scripts=[
   #        'scripts/cool',
   #        'scripts/skype',
   #       ]
)