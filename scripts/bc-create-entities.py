import argparse
import queue
import threading
import tqdm
from time import sleep

import neo4j

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
parser.add_argument('-b', '--batch-size', default=50,
                    help='Number of blocks to query at the same time')
parser.add_argument('--start-height', default=0,
                    help='At which block to start')
parser.add_argument('--max-height', default=None,
                    help="At which block to end")


def fetch_outputs_thread(session: neo4j.Session, batch_size: int, start_height: int, max_height: int,
                         result_queue: queue.Queue, stop_queue: queue.Queue):
    query = """
        MATCH (b:Block)
        WHERE b.height >= $lower AND b.height < $higher
        WITH b
        LIMIT $higher-$lower
        MATCH (b)-[:CONTAINS]->(t)<-[:INPUT]-(o)-[:USES]->(a)
        RETURN t.txid as txid, collect(a.address) as addresses
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


def main(host, port, user, password, batch_size, start_height, max_height):
    driver = neo4j.GraphDatabase.driver(f"bolt://{host}:{port}",
                                        auth=(user, password),
                                        connection_timeout=3600)

    entity_idx_counter = 0
    entity_idx_to_addresses = {}  # in theory, could be a list, but slightly more complex logic for little gain
    address_to_entity_idx = {}

    session = driver.session()
    result_queue = queue.Queue()
    stop_queue = queue.Queue()

    if max_height is None:
        max_height = session.run("MATCH (b:Block) RETURN max(b.height) as maxHeight").data()[0]["maxHeight"]

    # This is the thread that continuously queries for the next batch_size blocks, and returns the outputs + addresses
    thread = threading.Thread(target=fetch_outputs_thread,
                              args=(session, batch_size, start_height, max_height, result_queue, stop_queue,))
    thread.start()

    counter_joined_entities = 0
    counter_entities = 0

    try:
        loop_counter = 0
        progress_bar = tqdm.tqdm(total=max_height)
        while True:
            try:
                result_list = result_queue.get_nowait()
                if result_list is None:
                    break
                for result in result_list:
                    txid = result["txid"]
                    addresses = result["addresses"]

                    found_entities_idx = []
                    for addr in addresses:
                        entity_idx = address_to_entity_idx.get(addr, None)
                        if entity_idx is not None:
                            found_entities_idx.append(entity_idx)

                    if found_entities_idx:
                        # Here we need to join all the addresses from the different entities together
                        min_entity_idx = min(found_entities_idx)
                        entity_address_set = entity_idx_to_addresses[min_entity_idx]
                        for entity_idx in found_entities_idx:
                            if entity_idx == min_entity_idx:
                                continue
                            entity_address_set.update(entity_idx_to_addresses[entity_idx])
                            entity_idx_to_addresses.pop(entity_idx)

                        for addr in addresses:
                            address_to_entity_idx[addr] = min_entity_idx

                        counter_entities -= (len(found_entities_idx) - 1)
                        counter_joined_entities += len(found_entities_idx)

                    else:
                        if len(addresses) <= 1:
                            continue
                        # create a new entity
                        entity_idx = entity_idx_counter
                        for addr in addresses:
                            address_to_entity_idx[addr] = entity_idx
                        entity_idx_to_addresses[entity_idx] = set(addresses)
                        entity_idx_counter += 1
                        counter_entities += 1

                progress_bar.update(batch_size)

            except queue.Empty:
                print("-", end="")
                sleep(1)

            loop_counter += 1
            # if loop_counter % 50 == 0:
            progress_bar.set_postfix({'Total entities': counter_entities, 'Counter joined': counter_joined_entities})

    except KeyboardInterrupt:
        stop_queue.put("Time to stop")
        for i in range(10):
            if thread.is_alive():
                sleep(2)

    with driver.session() as session:
        for entity_idx, addresses in entity_idx_to_addresses.items():
            if len(addresses) <= 1:
                continue
            session.run("""
            MATCH (a:Address)
            WHERE a.address in $addresses
            WITH a
            OPTIONAL MATCH (e:Entity)--(a)
            CALL {
                WITH a,e
                WITH a,e as entity WHERE entity IS NOT NULL
                MERGE (a)-[:BELONGS_TO]->(entity)
                RETURN entity
                
                UNION 
                
                WITH a,e
                WITH a,e WHERE e IS NULL
                WITH collect(a) as addresses
                MERGE (entity:Entity {representative: $representative})
                WITH entity, addresses
                UNWIND addresses as a
                MERGE (entity)<-[:BELONGS_TO]-(a)
                RETURN entity
            }
            RETURN entity
            """, addresses=list(addresses), representative=min(addresses))

    driver.close()


if __name__ == "__main__":
    args = parser.parse_args()
    main(**vars(args))
