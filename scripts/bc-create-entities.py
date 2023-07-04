#!/usr/bin/env python

import argparse

import neo4j

from bitcoingraph.entities import add_entities

parser = argparse.ArgumentParser(
    description='Synchronise database with blockchain')

parser.add_argument('-H', '--host', default='localhost',
                    help='Neo4j host')
parser.add_argument('-P', '--port', default='7687',
                    help='Neo4j Bolt port')
parser.add_argument('-u', '--user', required=True,
                    help='Neo4j username')
parser.add_argument('-p', '--password', required=True,
                    help='Neo4j password')
parser.add_argument('--protocol', default='bolt://',
                    help="Protocol to use to connect to db. Default to bolt://")
parser.add_argument('-b', '--batch-size', default=50, type=int,
                    help='Number of blocks to query at the same time')
parser.add_argument('--start-height', default=0, type=int,
                    help='At which block to start')
parser.add_argument('--max-height', default=None, type=int,
                    help="At which block to end")
parser.add_argument('--resume', default=None, type=str,
                    help="If provided, resumes from given file")


def main(host, port, user, password, batch_size, start_height, max_height, protocol, resume):
    driver = neo4j.GraphDatabase.driver(f"{protocol}{host}:{port}",
                                        auth=(user, password),
                                        connection_timeout=3600)

    add_entities(batch_size, start_height, max_height, resume, driver)
    driver.close()


if __name__ == "__main__":
    args = parser.parse_args()
    main(**vars(args))
