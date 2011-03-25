#!/usr/bin/env python

from distutils.core import setup, Extension
import distutils.sysconfig
from distutils.debug import DEBUG
import sys

setup(name = 'ClusterStorageLib', 
    version = '0.1.0', 
    description = 'Library to handle cluster storage system more robustly',
    author = 'Marc Hulsman',
    author_email = 'm.hulsman@tudelft.nl',
    url ='',
    py_modules = ['cluster_storage'],
    provides=['cluster_storage'],
    scripts = ['gcp','gls','gmkdir','grmdir','greplicate','grm']
)


