import argparse
import queue
import threading
from itertools import islice

import tqdm
from time import sleep

import neo4j
from bitcoinlib.keys import Key

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
parser.add_argument('-b', '--batch-size', default=50,
                    help='Number of blocks to query at the same time')
parser.add_argument('--start-height', default=0,
                    help='At which block to start')
parser.add_argument('--max-height', default=None,
                    help="At which block to end")


def fetch_address_by_block_thread(session: neo4j.Session, batch_size: int, start_height: int, max_height: int,
                                  result_queue: queue.Queue, stop_queue: queue.Queue):
    query = """
        MATCH (b:Block)
        WHERE b.height >= $lower AND b.height < $higher
        WITH b
        LIMIT $higher-$lower
        MATCH (b)-[:CONTAINS]->(t)-[:OUTPUT]->(o)-[:USES]->(a)
        WHERE a.address STARTS WITH "pk_" AND NOT EXISTS((a)-[:GENERATES]->())
        RETURN b.height, collect(distinct a.address) as addresses
        """

    idx = start_height
    try:
        while stop_queue.empty() and idx < max_height:
            result = session.run(query, stream=True, lower=idx, higher=idx + batch_size)
            data = result.data()
            result_queue.put(data)
            idx += batch_size

    except Exception as e:
        print(f"Thread failed: {e}")
    finally:
        result_queue.put(None)
        session.close()


def get_p2pkh(k: Key):
    return k.address()


def get_p2wpkh(k: Key):
    return k.address(compressed=True, encoding="bech32")


def main(host, port, user, password, batch_size, start_height, max_height, protocol):
    driver = neo4j.GraphDatabase.driver(f"{protocol}{host}:{port}",
                                        auth=(user, password),
                                        connection_timeout=3600)

    result_queue = queue.Queue()
    stop_queue = queue.Queue()
    session = driver.session()
    try:
        if max_height is None:
            max_height = session.run("MATCH (b:Block) RETURN max(b.height) as maxHeight").data()[0]["maxHeight"]

        print(f"Running with {start_height} <= height < {max_height}")

        thread = threading.Thread(target=fetch_address_by_block_thread,
                                  args=(session, batch_size, start_height, max_height, result_queue, stop_queue,))
        thread.start()

        pk_to_addresses = {}
        progress_bar = tqdm.tqdm(total=max_height - start_height)
        while True:
            try:
                result_list = result_queue.get_nowait()
                if result_list is None:
                    break

                for row in result_list:
                    addresses = row["addresses"]

                    for addr in addresses:
                        if addr.endswith("CHECKSIG"):
                            pk = addr[3:-12]
                        else:
                            pk = addr[3:]

                        try:
                            pk_key = Key(pk)
                        except Exception:
                            continue

                        pk_to_addresses[addr] = [get_p2pkh(pk_key), get_p2wpkh(pk_key)]

                progress_bar.update(batch_size)
            except queue.Empty:
                sleep(0.5)

            except KeyboardInterrupt:
                pass

        progress_bar.set_description(f"Finished processing {len(pk_to_addresses)} addresses, saving results")

        query = """
        UNWIND $pk_map AS addresses
        WITH addresses[0] as pk, tail(addresses) as list_generated
        MATCH (a:Address {address: pk})
        WITH a, list_generated
        UNWIND list_generated as generated
            MERGE (b:Address {address: generated})
            MERGE (a)-[:GENERATES]->(b)
        """
        with driver.session() as session:
            data = [[pk, addrs[0], addrs[1]] for pk, addrs in pk_to_addresses.items()]
            for i in range(0, len(data), batch_size):
                session.run(query, pk_map=data[i:i+batch_size])

    finally:
        driver.close()


if __name__ == "__main__":
    args = parser.parse_args()
    main(**vars(args))
