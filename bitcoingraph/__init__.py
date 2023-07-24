"""
bitcoingraph

A Python library for exploring the Bitcoin transaction graph.

"""
import platform

from bitcoingraph.bitcoingraph import BitcoinGraph

__author__ = 'Bernhard Haslhofer, Roman Karl'
__license__ = "MIT"
__version__ = '0.3.2dev'

if platform.python_implementation() == "PyPy":
    print("WARNING: You are running using pypy. Neo4j driver is not compatible with it, so "
          "all neo4j related functions will fail. Pypy can only be used to speed up computes "
          "that do not include neo4j, such as bcgraph-compute-entities")