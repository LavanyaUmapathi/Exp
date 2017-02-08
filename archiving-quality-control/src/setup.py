'''
Created on Apr 14, 2016

@author: natasha.gajic
'''
from setuptools import setup, find_packages
# To use a consistent encoding
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))
setup(
      name='ods quality control',
      version='0.0.1',
      packages=find_packages()
      )