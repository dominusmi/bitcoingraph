import bisect
import csv
import os
import pickle
import queue
import threading
import uuid
from pathlib import Path
from time import sleep

import neo4j
import tqdm


class Address:
    counter = 0

    def __init__(self, address, assign_number=False):
        self.address = address
        self.representative = None
        self.height = 0
        if assign_number:
            self.number = Address.counter
            Address.counter += 1

    def get_representative(self):
        r = self
        while r.representative is not None:
            r = r.representative
        return r

    def set_representative(self, address):
        self.representative = address
        if self.height >= address.height:
            address.height = self.height + 1

    def __lt__(self, other):
        return self.address < other.address

    def __eq__(self, other):
        return self.address == other.address

    def __hash__(self):
        return hash(self.address)


class AddressList:

    def __init__(self):
        self.addresses = []

    def add(self, address_string):
        self.addresses.append(Address(address_string, True))

    def group(self, address_strings):
        if len(address_strings) >= 2:
            addresses = list(map(self.search, address_strings))
            representatives = {address.get_representative() for address in addresses}
            highest_representative = None
            for representative in representatives:
                if (highest_representative is None or
                        representative.height > highest_representative.height):
                    highest_representative = representative
            representatives.remove(highest_representative)
            for representative in representatives:
                representative.set_representative(highest_representative)

    def search(self, address_string):
        index = bisect.bisect_left(self.addresses, Address(address_string))
        return self.addresses[index]

    def export(self, path):
        with open(os.path.join(path, 'entities.csv'), 'w') as entity_csv_file, \
                open(os.path.join(path, 'rel_address_entity.csv'), 'w') as entity_rel_csv_file:
            entity_writer = csv.writer(entity_csv_file)
            entity_rel_writer = csv.writer(entity_rel_csv_file)
            entity_writer.writerow(['id:ID(Entity)'])
            entity_rel_writer.writerow([':START_ID(Address)', ':END_ID(Entity)'])
            for address in self.addresses:
                representative = address.get_representative()
                if address == representative:
                    entity_writer.writerow([representative.number])
                entity_rel_writer.writerow([address.address, representative.number])

    def print(self):
        for address in self.addresses:
            print(address.address, address.get_representative().address)


def compute_entities(input_path):
    address_list = AddressList()
    print('reading addresses')
    with open(os.path.join(input_path, 'addresses.csv'), 'r') as address_file:
        for line in address_file:
            line = line.strip()
            address_list.add(line)
    print('reading inputs')
    input_counter = 0
    with open(os.path.join(input_path, 'input_addresses.csv'), 'r') as input_file:
        input_addresses = set()
        transaction = None
        for line in input_file:
            entries = line.strip().split(',')
            address = entries[1]
            if transaction is None or transaction == entries[0]:
                input_addresses.add(address)
            else:
                address_list.group(input_addresses)
                input_addresses = {address}
            transaction = entries[0]
            input_counter += 1
            if input_counter % (1000 * 1000) == 0:
                print('processed inputs:', input_counter)
        address_list.group(input_addresses)
    print('write to file')
    address_list.export(input_path)


def open_csv(input_path, base_name, mode):
    return open(os.path.join(input_path, base_name + '.csv'), mode, newline='')


def calculate_input_addresses(input_path):
    print('calculating input addresses')
    with open_csv(input_path, 'rel_input', 'r') as input_file, \
            open_csv(input_path, 'rel_output_address', 'r') as output_address_file, \
            open_csv(input_path, 'input_addresses', 'w') as input_addresses_file:
        input_reader = csv.reader(input_file)
        output_address_reader = csv.reader(output_address_file)
        input_address_writer = csv.writer(input_addresses_file)
        last_output = ''
        last_address = ''
        for input_row in input_reader:
            txid = input_row[0]
            output_ref = input_row[1]

            match_address = last_address if output_ref == last_output else None

            if output_ref >= last_output:
                for output_row in output_address_reader:
                    output = output_row[0]
                    address = output_row[1]
                    if output_ref == output:
                        if match_address is None:
                            match_address = address
                        else:
                            match_address = None
                            last_output = output
                            last_address = address
                            break
                    elif output_ref < output:
                        last_output = output
                        last_address = address
                        break
                    last_output = output
                    last_address = address

            if match_address is not None:
                input_address_writer.writerow([txid, match_address])


def fetch_transactions_from_blocks(session: neo4j.Session, start_height: int, max_height: int):
    query = """
        MATCH (b:Block)-[:CONTAINS]->(t)<-[:INPUT]-(o)-[:USES]->(a)
        WHERE b.height >= $lower AND b.height < $higher
        WITH b
        MATCH (b)-[:CONTAINS]->(t)<-[:INPUT]-(o)-[:USES]->(a)
        WITH t, collect(distinct a.address) as addresses
        RETURN addresses
    """
    result = session.run(query, stream=True, lower=start_height, higher=max_height)
    return result.data()


def _fetch_outputs_thread(session: neo4j.Session, batch_size: int, skip: int,
                          result_queue: queue.Queue, stop_queue: queue.Queue):

    try:
        query = """
            MATCH (t:Transaction)
            WITH t
            SKIP $skip
            CALL {
                WITH t
                MATCH (t)<-[:INPUT]-()-[:USES]->(a)
                WITH t, collect(distinct a.address) as addresses
                RETURN addresses
            } IN TRANSACTIONS
            RETURN addresses
            """
        cursor = session.run(query, stream=True, skip=skip)

        while True:
            result = cursor.fetch(batch_size)
            if not result:
                break
            if not stop_queue.empty():
                raise Exception("Received stop signal")
            result_queue.put([{"addresses": g.get("addresses")} for g in result])

        result_queue.put(None)

    except Exception as e:
        print(f"Thread failed: {e}")
        result_queue.put(-1)
    finally:
        print("exiting")
        result_queue.put(None)
        session.close()


class EntityGrouping:
    def __init__(self):
        self.entity_idx_counter = 0
        self.entity_idx_to_addresses = {}  # in theory, could be a list, but slightly more complex logic for little gain
        self.address_to_entity_idx = {}
        self.entity_idx_counter = 0
        self.counter_entities = 0
        self.counter_joined_entities = 0

    def update_from_address_group(self, addresses):
        found_entities_idx = set([])
        for addr in addresses:
            entity_idx = self.address_to_entity_idx.get(addr, None)
            if entity_idx is not None:
                found_entities_idx.add(entity_idx)

        if found_entities_idx:
            # Here we need to join all the addresses from the different entities together
            min_entity_idx = min(found_entities_idx)
            entity_address_set = self.entity_idx_to_addresses[min_entity_idx]
            moved_addresses = set(addresses)
            for entity_idx in found_entities_idx:
                if entity_idx == min_entity_idx:
                    continue

                entity_addresses_to_merge = self.entity_idx_to_addresses.pop(entity_idx)
                moved_addresses.update(entity_addresses_to_merge)
                entity_address_set.update(entity_addresses_to_merge)

            for addr in moved_addresses:
                self.address_to_entity_idx[addr] = min_entity_idx
                self.entity_idx_to_addresses[min_entity_idx].add(addr)

            self.counter_entities -= (len(found_entities_idx) - 1)
            self.counter_joined_entities += len(found_entities_idx) - 1

        else:
            if len(addresses) <= 1:
                return
            # create a new entity
            entity_idx = self.entity_idx_counter
            for addr in addresses:
                self.address_to_entity_idx[addr] = entity_idx
            self.entity_idx_to_addresses[entity_idx] = set(addresses)
            self.entity_idx_counter += 1
            self.counter_entities += 1

    def save_entities(self, session: neo4j.Session, display_progress=False):
        if display_progress:
            iterator = tqdm.tqdm(self.entity_idx_to_addresses.items(), total=len(self.entity_idx_to_addresses),
                                 desc="Saving entities")
        else:
            iterator = self.entity_idx_to_addresses.items()
        for entity_idx, addresses in iterator:
            if len(addresses) <= 1:
                continue
            result = session.run("""
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

            result = result.consume()


def add_entities(batch_size: int, resume: str, driver: neo4j.Driver):
    session = driver.session()
    result_queue = queue.Queue()
    stop_queue = queue.Queue()

    if resume is not None:
        path = Path(resume).resolve()
        with open(path, "rb+") as f:
            data = pickle.load(f)
            current_transaction = data["iteration"]
            entity_grouping = data["grouping"]
            print(f"Resuming from {path} at transaction {current_transaction}")
    else:
        entity_grouping = EntityGrouping()
        current_transaction = 0

    # 2.69 is the average number of inputs per transactions. The real query would actually be
    # RETURN count(distinct t), however this takes a lot of time, hence the use of estimate
    count_transactions = session.run("MATCH (t:Transaction)<-[:INPUT]-() RETURN count(t) as ct").data()[0]["ct"] / 2.69
    count_transactions = int(round(count_transactions))

    # This is the thread that continuously queries for the next batch_size transactions
    thread = threading.Thread(target=_fetch_outputs_thread,
                              args=(session, batch_size, current_transaction, result_queue, stop_queue,))
    thread.start()

    dump_path = f"./state_dump_{uuid.uuid4()}.pickle"
    print(f"Dump file: {dump_path}")
    try:
        loop_counter = 0
        progress_bar = tqdm.tqdm(desc="Transactions read", total=count_transactions-current_transaction)
        while True:
            try:
                result_list = result_queue.get_nowait()
                if result_list is None:
                    break

                if result_list == -1:
                    # This only happens if the thread failed due to an exception
                    # In this case we try to start it again
                    print("Thread died. Trying to restart it")
                    expect_none = result_queue.get() # We must receive a None othw something went wrong and it's better to shutdown
                    assert expect_none is None
                    sleep(5)
                    thread = threading.Thread(target=_fetch_outputs_thread,
                                              args=(driver.session(), batch_size, current_transaction, result_queue, stop_queue,))
                    thread.start()
                    continue

                for result in result_list:
                    addresses = result["addresses"]
                    entity_grouping.update_from_address_group(addresses)

                batch_transaction = len(result_list)
                current_transaction += batch_transaction
                progress_bar.update(batch_transaction)

            except queue.Empty:
                print("-", end="")
                sleep(1)

            loop_counter += 1
            progress_bar.set_postfix({'Total entities': len(entity_grouping.entity_idx_to_addresses),
                                      'Counter joined': entity_grouping.counter_joined_entities})

            if loop_counter % int(round(500000 / batch_size)) == 0:
                with open(dump_path, "wb+") as f:
                    print("Dumping current state")
                    pickle.dump({"iteration": current_transaction, "grouping": entity_grouping}, f)

    except KeyboardInterrupt:
        stop_queue.put("Time to stop")
        for i in range(10):
            if thread.is_alive():
                sleep(2)

    with driver.session() as session:
        entity_grouping.save_entities(session, display_progress=True)
