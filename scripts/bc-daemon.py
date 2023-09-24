#!/usr/bin/env python

import argparse
import queue
import sys
from time import sleep

from bitcoingraph.bitcoingraph import BitcoinGraph
from bitcoingraph.blockchain import BlockchainException
from bitcoingraph.logger import get_logger
logger = get_logger("bc-daemon")

parser = argparse.ArgumentParser(
    description='Synchronise database with blockchain')
parser.add_argument('-s', '--bc-host', required=True,
                    help='Bitcoin Core host')
parser.add_argument('--bc-port', default='8332',
                    help='Bitcoin Core port')
parser.add_argument('-u', '--bc-user', required=True,
                    help='Bitcoin Core RPC username')
parser.add_argument('-p', '--bc-password', required=True,
                    help='Bitcoin Core RPC password')
parser.add_argument('--rest', action='store_true',
                    help='Prefer REST API over RPC. This is only possible on localhost.')
parser.add_argument('-S', '--neo4j-host', required=True,
                    help='Neo4j host')
parser.add_argument('--neo4j-port', default='7687',
                    help='Neo4j port')
parser.add_argument('-U', '--neo4j-user', required=True,
                    help='Neo4j username')
parser.add_argument('-P', '--neo4j-password', required=True,
                    help='Neo4j password')
parser.add_argument('--neo4j-protocol', default="bolt://",
                    help='Neo4j protocol. Defaults to bolt://')
parser.add_argument('-b', '--max-height', type=int, default=1_000_000_000_000,
                    help='Max block height to reach')
parser.add_argument('-l', '--lag', type=int, required=True,
                    help='How many blocks to keep in safety buffer in case of re-organisation')


def thread_synchronization(bcgraph: BitcoinGraph, max_height: int, queue_new_block: queue.Queue,
                           queue_stop: queue.Queue):

    while True:
        try:
            # check whether we received a stop signal from the daemon
            if not queue_stop.empty():
                return

            for height in bcgraph.synchronize(max_height):
                queue_new_block.put(height)

        except BlockchainException as e:
            print(f"Blockchain error: {e}. Trying again soon")

        except StopIteration:
            print("Finished synchronize")
            break

        finally:
            # try again in 5 seconds
            sleep(5)

    queue_new_block.put(None)

def thread_wrapper(f, data_queue: queue.Queue, stop_queue: queue.Queue):
    while True:
        try:
            data = data_queue.get_nowait()
            f(data)

        except queue.Empty:
            if not stop_queue.empty():
                return
            sleep(0.5)


seen_addresses = set([])

def main(bc_host, bc_port, bc_user, bc_password, rest, neo4j_host, neo4j_port, neo4j_user, neo4j_password,
         neo4j_protocol, max_height, lag):
    blockchain = {'host': bc_host, 'port': bc_port,
                  'rpc_user': bc_user, 'rpc_pass': bc_password}
    if rest:
        blockchain['method'] = 'REST'
    neo4j_cfg = {'host': neo4j_host, 'port': neo4j_port,
                 'user': neo4j_user, 'pass': neo4j_password}

    if lag <= 4:
        logger.warn(f"You have chosen a lag of {lag} which is below the recommended 5. This is considered dangerous as, "
                    f"if there's any re-organisation of the block maximum-{lag}, then the block will already have been "
                    f"loaded on neo4j, and it will crash the daemon. The latest blocks will have to be manually removed "
                    f"and added again. It is strongly recommended to keep a lag > 4")
        if input("I understand the risk and want to proceed [y/n]").strip().lower() != "y":
            logger.info("Exiting")
            sys.exit()


    finished_sync = False
    bcgraph = BitcoinGraph(blockchain=blockchain, neo4j=neo4j_cfg)
    logger.info(f"Starting daemon. Running with max height {max_height}")
    while True:
        for _ in bcgraph.synchronize(max_height, lag):
            finished_sync = False

        if not finished_sync:
            finished_sync = True
            logger.info("Finished syncing, sleeping")

        sleep(5)


if __name__ == "__main__":
    args = parser.parse_args()
    main(**vars(args))
