from __future__ import print_function

import platform

from setuptools import setup
from setuptools.command.test import test as TestCommand
import io
import sys

import bitcoingraph


def read(*filenames, **kwargs):
    encoding = kwargs.get('encoding', 'utf-8')
    sep = kwargs.get('sep', '\n')
    buf = []
    for filename in filenames:
        with io.open(filename, encoding=encoding) as f:
            buf.append(f.read())
    return sep.join(buf)


long_description = read('README.rst')

_compatible_install = [
    'neo4j>=4',
    'requests>=2.5.0',
    'tqdm~=4.65.0'
]

if platform.python_implementation() == "PyPy":
    print("Skipping install neo4j because of PyPy error")
    _compatible_install.pop(0)


class PyTest(TestCommand):
    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = []
        self.test_suite = True

    def run_tests(self):
        import pytest
        errcode = pytest.main(self.test_args)
        sys.exit(errcode)


setup(
    # Basic info
    name='bitcoingraph',
    version=bitcoingraph.__version__,
    url='https://github.com/behas/bitcoingraph.git',

    # Author
    author='Bernhard Haslhofer',
    author_email='bernhard.haslhofer@ait.ac.at',

    # Description
    description="""A Python library for extracting and navigating
                   graphstructures from the Bitcoin block chain.""",
    long_description=long_description,

    # Package information
    packages=['bitcoingraph'],
    scripts=['scripts/bcgraph-export',
             'scripts/bcgraph-compute-entities',
             'scripts/bcgraph-synchronize'],
    platforms='any',
    install_requires=[
        'requests>=2.5.0',
        'tqdm~=4.65.0',
    ],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3',
        'Topic :: Software Development :: Libraries',
    ],

    # Testing
    test_suite='tests',
    tests_require=['pytest'],
    cmdclass={'test': PyTest},
    extras_require={
        'testing': ['pytest'],
        'ssh': ['paramiko']
    },

    # Legal info
    license='MIT License',
)
