#!/usr/bin/env python

from setuptools import setup, find_packages


f = open('VERSION', 'r')
version = f.readline().strip()
f.close()

author = 'David-Leon Pohl, Jens Janssen'
author_email = 'pohl@physik.uni-bonn.de, janssen@physik.uni-bonn.de'

# requirements for core functionality
install_requires = ['tables']

setup(
    name='PyTables2RootTTree',
    version=version,
    description='PyTables2RootTTree - Converter from HDF5/pytables table to CERN ROOT TTree',
    url='https://github.com/SiLab-Bonn/PyTables2RootTTree',
    license='MIT License (MIT)',
    long_description='',
    author=author,
    maintainer=author,
    author_email=author_email,
    maintainer_email=author_email,
    install_requires=install_requires,
    packages=find_packages(),
    include_package_data=True,
    package_data={'': ['README.*', 'VERSION', 'LICENSE']},
    platforms='any'
)
