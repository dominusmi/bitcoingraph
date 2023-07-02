import argparse
import queue
import threading
from time import sleep

import neo4j
import tqdm

from bitcoingraph.address import process_create_pk_to_generated

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


def thread_get_entities_per_block(session: neo4j.Session, max_height: int, batch_size: int, result_queue: queue.Queue,
                                  stop_queue: queue.Queue):
    idx = 0
    try:
        while stop_queue.empty() and idx < max_height:
            result = session.run("""
            MATCH (b:Block)
            WHERE b.height >= $lower AND b.height < $higher
            WITH b
            LIMIT $higher-$lower
            MATCH (b)-[:CONTAINS]->()<-[:INPUT]-()-[:USES]->()-[:BELONGS_TO]->(e)
            RETURN collect(distinct elementId(e)) as relIds
            """, lower=idx, higher=idx + batch_size).data()[0]["relIds"]
            result_queue.put(result)
            idx += batch_size

    except Exception as e:
        print(f"Thread failed: {e}")
    finally:
        stop_queue.put(True)
        result_queue.put(None)
        session.close()


def thread_delete_entities(session: neo4j.Session, data_queue: queue.Queue, stop_queue: queue.Queue):
    is_empty_counter = 0
    try:
        while True:
            try:
                new_entities = data_queue.get_nowait()
                print(f"Deleting {len(new_entities)}")
                session.run("""
                MATCH (e)
                WHERE elementId(e) in $elmIds
                DETACH DELETE e
                """, elmIds=list(new_entities))

            except queue.Empty:
                sleep(0.5)
                if not stop_queue.empty():
                    is_empty_counter += 1
                    if is_empty_counter > 4:
                        print("Stopping entities threas")
                        break
    finally:
        session.close()
    return


def main(host, port, user, password, batch_size, start_height, max_height, protocol):
    driver = neo4j.GraphDatabase.driver(f"{protocol}{host}:{port}",
                                        auth=(user, password),
                                        connection_timeout=3600)

    result_queue = queue.Queue()
    stop_queue = queue.Queue()
    thread_session = driver.session()

    if max_height is None:
        max_height = thread_session.run("MATCH (b:Block) RETURN max(b.height) as maxHeight").data()[0]["maxHeight"]

    print(f"Running with {start_height} <= height < {max_height}")

    thread = threading.Thread(target=thread_get_entities_per_block,
                              args=(thread_session, max_height, batch_size, result_queue, stop_queue,))
    thread.start()

    entities_queue = queue.Queue()
    thread = threading.Thread(target=thread_delete_entities,
                              args=(driver.session(), entities_queue, stop_queue,))
    thread.start()

    session = driver.session()
    progress_bar = tqdm.tqdm(total=max_height - start_height)
    seen_rels = set([])
    batch_rels = set([])
    try:
        while True:
            try:
                relationships = result_queue.get_nowait()
                if relationships is None:
                    break

                new_rels = set(relationships).difference(seen_rels)
                seen_rels.update(new_rels)
                if new_rels:
                    batch_rels.update(new_rels)
                    if len(batch_rels) >= 2000:
                        entities_queue.put(batch_rels)
                        batch_rels = set([])

                progress_bar.update(batch_size)
                progress_bar.set_postfix({"awaiting rels": len(batch_rels)})

            except queue.Empty:
                sleep(0.5)

            except KeyboardInterrupt:
                pass

    finally:
        entities_queue.put(batch_rels)
        stop_queue.put(True)
        thread.join()
        session.close()
        driver.close()


if __name__ == "__main__":
    args = parser.parse_args()
    main(**vars(args))
