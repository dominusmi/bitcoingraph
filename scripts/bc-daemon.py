#!/usr/bin/env python

import argparse
import queue
import threading
from time import sleep

import neo4j

from bitcoingraph.bitcoingraph import BitcoinGraph
from bitcoingraph.address import fetch_addresses_by_block, generate_from_address_list, \
    save_generated_addresses
from bitcoingraph.blockchain import BlockchainException
from bitcoingraph.entities import fetch_transactions_from_blocks, EntityGrouping

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
parser.add_argument('-b', '--max-blocks', type=int, default=1_000_000_000_000,
                    help='Enforce a limit on the number of blocks that are synchronised')


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


def generate_addresses(session: neo4j.Session, start_height: int, max_height: int):
    global seen_addresses
    pk_to_addresses = {}
    addresses = fetch_addresses_by_block(session, start_height, max_height)

    # avoids reprocessing the same addresses
    set_addresses = set(addresses)
    to_process = set_addresses.difference(seen_addresses)
    seen_addresses.update(set_addresses)
    addresses = list(to_process)

    if len(pk_to_addresses) == 0:
        generate_from_address_list(addresses, pk_to_addresses)
        print(f"Saving {len(addresses) * 2} generated addresses {start_height} -> {max_height}")
        save_generated_addresses(session, pk_to_addresses)


def upsert_entities(session: neo4j.Session, start_height: int, max_height: int):
    print(f"Fetching addresses for entity creation, block {start_height} -> {max_height}")
    rows = fetch_transactions_from_blocks(session, start_height, max_height)
    entity_grouping = EntityGrouping()
    for row in rows:
        addresses = row["addresses"]
        entity_grouping.update_from_address_group(addresses)

    print(f"Saving {len(entity_grouping.entity_idx_to_addresses)} entities {start_height} -> {max_height}")

    entity_grouping.save_entities(session)


def main(bc_host, bc_port, bc_user, bc_password, rest, neo4j_host, neo4j_port, neo4j_user, neo4j_password,
         neo4j_protocol, max_blocks):
    print("Startin Daemon")
    driver = neo4j.GraphDatabase.driver(f"{neo4j_protocol}{neo4j_host}:{neo4j_port}",
                                        auth=(neo4j_user, neo4j_password),
                                        connection_timeout=3600)

    blockchain = {'host': bc_host, 'port': bc_port,
                  'rpc_user': bc_user, 'rpc_pass': bc_password}
    if rest:
        blockchain['method'] = 'REST'
    neo4j_cfg = {'host': neo4j_host, 'port': neo4j_port,
                 'user': neo4j_user, 'pass': neo4j_password}

    bcgraph = BitcoinGraph(blockchain=blockchain, neo4j=neo4j_cfg)

    new_block_queue = queue.Queue()
    sync_stop_queue = queue.Queue()

    thread = threading.Thread(target=thread_synchronization,
                              args=(bcgraph, max_blocks, new_block_queue, sync_stop_queue))
    thread.start()

    post_process_stop_queue = queue.Queue()
    addresses_session = driver.session()
    addresses_queue = queue.Queue()
    addresses_thread = threading.Thread(target=thread_wrapper,
                                        args=(
                                        lambda args: generate_addresses(addresses_session, *args), addresses_queue,
                                        post_process_stop_queue))
    addresses_thread.start()

    entities_session = driver.session()
    entities_queue = queue.Queue()
    upsert_thread = threading.Thread(target=thread_wrapper,
                                     args=(lambda args: upsert_entities(entities_session, *args), entities_queue,
                                           post_process_stop_queue))
    upsert_thread.start()

    stop_signal = False
    try:
        while True:
            try:
                blocks = []
                count_tries = 0
                while True:
                    try:
                        block = new_block_queue.get_nowait()
                        if block is None:
                            stop_signal = True
                            break
                        blocks.append(block)
                        if len(blocks) > 500 or stop_signal:
                            # max batch size of 50 blocks
                            break
                    except queue.Empty:
                        if stop_signal:
                            break

                        count_tries += 1
                        if count_tries > 10:
                            break
                        sleep(1)

                if blocks:
                    start_height = blocks[0]
                    end_height = blocks[-1]
                    print(f"Synchronized blocks {start_height} -> {end_height}. Launching post-processing")
                    addresses_queue.put((start_height, end_height))
                    entities_queue.put((start_height, end_height))
                    if stop_signal:
                        post_process_stop_queue.put(True)
                        print("Sent stop signal to post-processing and joining")
                        addresses_thread.join()
                        upsert_thread.join()

                if stop_signal:
                    # allows finishing one entire loop, and then exit
                    break
            except KeyboardInterrupt:
                sync_stop_queue.put(True)
                post_process_stop_queue.put(True)
                stop_signal = True
                print("Sent stop signal")

    finally:
        print("Cleaning up")
        addresses_session.close()
        entities_session.close()
        driver.close()


if __name__ == "__main__":
    args = parser.parse_args()
    main(**vars(args))
