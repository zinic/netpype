# -*- coding: utf-8 -*-
try:
    from setuptools import setup, find_packages
    from multiprocessing import util
except ImportError:
    from ez_setup import use_setuptools
    use_setuptools()
    from setuptools import setup, find_packages

setup(
    name = 'netpype',
    version = '0.1',
    description = '',
    author = 'John Hopper',
    author_email = '',
    install_requires = [
        "librabbitmq",
        "kombu",
        "mock"
    ],
    test_suite = 'queuet.tests',
    zip_safe = False,
    include_package_data = True,
    packages = find_packages(exclude=['ez_setup'])
)

