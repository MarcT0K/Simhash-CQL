#!/usr/bin/env python3

from setuptools import setup, find_packages

setup(
    name='simhash',
    version='2.0.0',
    keywords=('simhash'),
    description='A Python implementation of Simhash Algorithm',
    license='MIT License',

    url='http://leons.im/posts/a-python-implementation-of-simhash-algorithm/',
    author='1e0n, Baudroie Team',
    author_email='',

    packages=find_packages(),
    include_package_data=True,
    platforms='any',
    install_requires=[],
    tests_require=[
        'pytest',
        'numpy',
        'scipy',
        'scikit-learn',
    ],
    test_suite="pytest",
)
