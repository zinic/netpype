# -*- coding: utf-8 -*-
try:
    from setuptools import setup, find_packages
    from multiprocessing import util
except ImportError:
    from ez_setup import use_setuptools
    use_setuptools()
    from setuptools import setup, find_packages

from distutils.core import setup
from distutils.extension import Extension
from Cython.Distutils import build_ext

setup(
    cmdclass={'build_ext': build_ext},
    ext_modules=[Extension("netpype.cutil", ["netpype/cutil.pyx"], extra_compile_args=['-O2'])]
)

setup(
    name = 'netpype',
    version = '0.1',
    description = '',
    author = 'John Hopper',
    author_email='',
    tests_require=[
        "mock",
        "nose",
    ],
    install_requires=[
        "cython",
    ],
    test_suite = 'nose.collector',
    zip_safe=False,
    include_package_data=True,
    packages=find_packages(exclude=['ez_setup'])
)

