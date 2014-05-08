#!/usr/bin/python

from distutils.core import setup

setup(name='scalaris',
      version='0.7.0+svn',
      description='Scalaris python bindings',
      author='Nico Kruber',
      author_email='kruber@zib.de',
      url='https://code.google.com/p/scalaris/',
      py_modules=['scalaris', 'scalaris_bench'],
     )