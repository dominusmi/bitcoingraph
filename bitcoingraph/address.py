import queue
import threading
from time import sleep
from typing import List, Dict

import neo4j
import tqdm
from bitcoinlib.keys import Key


def fetch_addresses_by_block(session: neo4j.Session, start_height, max_height) -> List[str]:
    """
    Returns all the addresses used in a given block
    """
    query = """
        MATCH (b:Block)
        WHERE b.height >= $lower AND b.height <= $higher
        WITH b
        MATCH (b)-[:CONTAINS]->(t)-[:OUTPUT]->(o)-[:USES]->(a)
        WHERE a.address STARTS WITH "pk_" AND NOT EXISTS((a)-[:GENERATES]->())
        RETURN collect(distinct a.address) as addresses
        """
    addresses = session.run(query, stream=True, lower=start_height, higher=max_height).single()
    return addresses[0] if addresses else []


def _fetch_address_by_block_thread(session: neo4j.Session, batch_size: int, start_height: int, max_height: int,
                                   result_queue: queue.Queue, stop_queue: queue.Queue):
    idx = start_height
    try:
        while stop_queue.empty() and idx < max_height:
            data = fetch_addresses_by_block(session, idx, idx + batch_size)
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


def generate_from_address_list(addresses: List[str], pk_to_addresses: Dict[str, List[str]]):
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


def save_generated_addresses(session: neo4j.Session, pk_to_addresses: Dict[str, List[str]], batch_size=2000):
    query = """
    UNWIND $pk_map AS addresses
    WITH addresses[0] as pk, tail(addresses) as list_generated
    MATCH (a:Address {address: pk})
    WITH a, list_generated
    UNWIND list_generated as generated
        MERGE (a)-[:GENERATES]->(b:Address {address: generated})
    """
    data = [[pk, addrs[0], addrs[1]] for pk, addrs in pk_to_addresses.items()]
    for i in range(0, len(data), batch_size):
        session.run(query, pk_map=data[i:i + batch_size])


def process_create_pk_to_generated(batch_size: int, start_height: int, max_height: int, driver: neo4j.Driver):
    result_queue = queue.Queue()
    stop_queue = queue.Queue()
    thread_session = driver.session()

    if max_height is None:
        max_height = thread_session.run("MATCH (b:Block) RETURN max(b.height) as maxHeight").data()[0]["maxHeight"]

    print(f"Running with {start_height} <= height < {max_height}")

    thread = threading.Thread(target=_fetch_address_by_block_thread,
                              args=(thread_session, batch_size, start_height, max_height, result_queue, stop_queue,))
    thread.start()

    pk_to_addresses = {}
    progress_bar = tqdm.tqdm(total=max_height - start_height)
    while True:
        try:
            addresses = result_queue.get_nowait()
            if addresses is None:
                break

            generate_from_address_list(addresses, pk_to_addresses)

            progress_bar.update(batch_size)
        except queue.Empty:
            sleep(0.5)

        except KeyboardInterrupt:
            pass

    progress_bar.set_description(f"Finished processing {len(pk_to_addresses)} addresses, saving results")

    with driver.session() as session:
        save_generated_addresses(session, pk_to_addresses)


def generate_addresses(session: neo4j.Session, start_height: int, max_height: int):
    pk_to_addresses = {}
    addresses = fetch_addresses_by_block(session, start_height, max_height)

    if len(pk_to_addresses) == 0:
        generate_from_address_list(addresses, pk_to_addresses)
        save_generated_addresses(session, pk_to_addresses)
