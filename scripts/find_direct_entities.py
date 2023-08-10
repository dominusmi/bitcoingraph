import argparse
import logging
import os
import pathlib
import time
from datetime import datetime
from queue import Queue
from threading import Thread

import treelib.exceptions
from neo4j import GraphDatabase
from treelib import Tree

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s',
)
logger = logging.getLogger("entities")
logger.setLevel(logging.INFO)

file_handler = logging.FileHandler(f"track-bitcoin-output-{datetime.utcnow().isoformat()}.log")
logger.addHandler(file_handler)

# @profile
def run_query(session, txid, result_queue, query):
    # Replace the following query with your specific query, using txid as a parameter
    result = session.run(query, txid=txid)
    result_queue.put((txid, result.values()))
    session.close()

# @profile
def parallel_execution(driver, thread_function, iterable):
    def wrapper(idx, f, input_queue: Queue, queue, session):
        while True:
            input = input_queue.get()
            if input is None:
                break
            else:
                ret = f(session, elm)
                queue.put((idx, ret))

    try:
        threads = []
        result_queues = [Queue() for _ in range(20)]
        sessions = [driver.session() for i in range(20)]
        input_queues = [Queue() for _ in range(20)]
        for i in range(20):
            thread = Thread(target=wrapper, args=(i, thread_function, input_queues[i], result_queues[i], sessions[i]))
            thread.start()
            threads.append(thread)


        for i, elm in enumerate(iterable):
            sent = False
            if i < 20:
                input_queues[i].put(elm)
                continue

            while True:
                for queue in result_queues:
                    if not queue.empty():
                        idx, result = queue.get()
                        input_queues[idx].put(elm)
                        yield result
                        sent=True
                        break
                if sent:
                    break
                time.sleep(0.1)

        for queue in input_queues:
            queue.put(None)

        # Wait for all threads to finish
        for thread in threads:
            thread.join()

        # Yield the results from the queue
        for result_queue in result_queues:
            while not result_queue.empty():
                rows = result_queue.get()[1]
                yield rows

    finally:
        [session.close() for session in sessions]

class Walker(Tree):
    def __init__(self):
        super().__init__()
        self.create_node(identifier="root")

    def add_root(self, output: str):
        try:
            self.create_node(identifier=output, parent="root")
        except treelib.exceptions.DuplicatedNodeIdError:
            pass

    def get_path(self, node_id):
        path = []
        node = self.get_node(node_id)
        if node is None:
            return None  # Node not found
        while node is not None:
            path.append(node.identifier)
            node = self.parent(node.identifier)
        return list(reversed(path))

    def expand_node(self, txid: str, new_output: str, dollar_amount, address):
        if self.get_node(new_output) is None:
            self.create_node(identifier=new_output, parent=txid, data={"$": dollar_amount, "address": address})
            return True
        else:
            return False

    def remove_path(self, txid):
        try:
            parent = self.parent(txid)
        except treelib.exceptions.NodeIDAbsentError:
            return 0
        if len(self.children(parent.identifier)) == 1 and parent.identifier != "root":
            return self.remove_path(parent.identifier)
        else:
            deleted = self.remove_node(txid)
            return deleted

class InputWalker(Walker):
    def get_next_input(self):
        # we don't iterate of leaves directly since it can change as the loop goes on. This
        # way we get a snapshot
        leaves = list(self.leaves())
        for node in leaves:
            if "_" not in node.identifier:
                yield node.identifier

    def expand_transactions(self):
        leaves = list(self.leaves())
        for leaf in leaves:
            txid = leaf.identifier.split("_")[0]
            if self.get_node(txid) is None:
                self.create_node(identifier=txid, parent=leaf.identifier)
            else:
                self.remove_node(leaf.identifier)

# @profile
def track_output(driver, start, end, min_amount):
    logger.info(f"Running between {start} and {end}. Minimum transaction amount: {min_amount}$")

    address_to_outputs = {}
    walker = InputWalker()
    with driver.session() as session:
        logger.info("Getting starting values")
        result = session.run("""
            MATCH (e:Entity {name: $entityName})--(a:Address)--(o:Output)
            WITH a.address as address, collect(o.txid_n) as o
            RETURN address, o
        """, entityName=start).values()

        for address, outputs in result:
            address_to_outputs[address] = []
            for o in outputs:
                address_to_outputs[address].append(o)
                walker.add_root(o)
        logger.info(f"Initial addresses: {address_to_outputs.keys()}")
        logger.info("Getting destination addresses")
        try:
            int(end)
            name = "entity_id"
        except:
            name = "name"

        end_addresses = session.run("""
        MATCH (e:Entity {%s: $destination})--(a:Address)
        RETURN collect(distinct a.address) as addresses
        """ % name, destination=end).values("addresses")
        end_addresses = set(end_addresses[0][0])

        logger.info(f"End addresses: {end_addresses}")


    query_template = """
    MATCH (a:Address)<-[:USES]-(input:Output)-[:INPUT]->(t:Transaction {txid: $txid})<-[:CONTAINS]-(b:Block)
    WHERE input.value  > $min_value
    WITH collect([input.txid_n, input.value, a.address]) as tuple, t,b
    WITH t.txid as txid,b.timestamp as ts,tuple
    RETURN txid, ts, tuple
    """

    def thread_function(session, txid):
        logger.debug(f"Running on {txid}")
        try:
            ts = session.run("MATCH (t:Transaction {txid: $txid})<-[:CONTAINS]-(b:Block) RETURN b.timestamp as ts", txid=txid).single().get("ts")
        except AttributeError:
            logger.warning(f"How can {txid} not have a block?!")
            return None
        btc_price = get_average_price(ts)
        result = session.run(query_template, txid=txid, min_value=min_amount/btc_price)
        return txid, result.values()

    previous_transactions = set([])
    logger.info("Start walk")
    # with driver.session() as session:
    for depth in range(1,100):
        # for txid in walker.get_next_input():
            # for txid in walker.get_next_input():
        added_through_entity = 0
        with driver.session() as session:
            query_through_entity = """
            MATCH (a:Address)<-[:OWNER_OF]-()-[r:OWNER_OF]->()
            WHERE a.address in $addresses
            WITH a, count(r) as cr
            WHERE cr < 1000
            WITH a
            MATCH (a)<-[:OWNER_OF]-()-[:OWNER_OF]->(b:Address)<-[r:USES]-()
            WHERE a <> b 
            WITH a, count(r) as cr
            WHERE cr < 1000
            MATCH (a)<-[:OWNER_OF]-()-[:OWNER_OF]->(b:Address)<-[:USES]-(o:Output)
            RETURN collect(distinct o.txid_n) as txid_n
            """
            addresses = {l.data["address"] for l in walker.leaves() if l.data is not None}
            if addresses:
                logger.debug(f"Getting outputs through entity for {addresses}")
                result = session.run(query_through_entity, addresses=list(addresses)).values()
                if result and result[0] and result[0][0]:
                    for o in result[0][0]:
                        added_through_entity += 1
                        walker.add_root(o)


        walker.expand_transactions()

        to_remove = set([])
        transactions = list(walker.get_next_input())
        logger.info(f"Expanding {len(transactions)} transactions. Depth: {depth}")

        logger.debug(f"TX in common: {len(previous_transactions.intersection(set(transactions)))}")
        previous_transactions = set(transactions)
        for txid,rows in parallel_execution(driver, thread_function, transactions):
            # rows = session.run(query_template, txid=txid).values()
            if not rows:
                deleted = walker.remove_path(txid)
                continue
            row = rows[0]
            ts = row[1]
            price = get_average_price(ts)
            for (txid_n, value, address) in row[2]:
                if address in end_addresses:
                    logger.info("Found destination")
                    walker.expand_node(txid, txid_n, value*price, address)
                    logger.info(walker.get_path(txid_n))
                    return
                if not walker.expand_node(txid, txid_n, value*price, address):
                    # implies the path is already checked and is therefore dead
                    to_remove.add(txid)

        if to_remove:
            for txid in to_remove:
                node = walker.get_node(txid)
                if node and len(walker.children(txid)) == 0:
                    logger.debug(f"Removing {txid}")
                    walker.remove_path(txid)



def get_average_price(timestamp):
    data = {
        "Aug 21": 47_130.4, "Jul 21": 41_553.7, "Jun 21": 35_026.9, "May 21": 37_298.6,
        "Apr 21": 57_720.3, "Mar 21": 58_763.7, "Feb 21": 45_164.0, "Jan 21": 33_108.1,
        "Dec 20": 28_949.4, "Nov 20": 19_698.1, "Oct 20": 13_797.3, "Sep 20": 10_776.1,
        "Aug 20": 11_644.2, "Jul 20": 11_333.4, "Jun 20": 9_135.4, "May 20": 9_454.8,
        "Apr 20": 8_629.0, "Mar 20": 6_412.5, "Feb 20": 8_543.7, "Jan 20": 9_349.1,
        "Dec 19": 7_196.4, "Nov 19": 7_546.6, "Oct 19": 9_152.6, "Sep 19": 8_284.3,
        "Aug 19": 9_594.4, "Jul 19": 10_082.0, "Jun 19": 10_818.6, "May 19": 8_558.3,
        "Apr 19": 5_320.8, "Mar 19": 4_102.3, "Feb 19": 3_816.6, "Jan 19": 3_437.2,
        "Dec 18": 3_709.4, "Nov 18": 4_039.7, "Oct 18": 6_365.9, "Sep 18": 6_635.2,
    }

    given_date = datetime.fromtimestamp(timestamp)
    date_format = "%b %y"
    closest_date = min(data.keys(), key=lambda x: abs(given_date - datetime.strptime(x, date_format)))
    return data[closest_date]

def main(driver, receiver, owner, min_amount):
    track_output(driver,  receiver, owner, min_amount)

parser = argparse.ArgumentParser()
parser.add_argument('receiver', help="Entity name or id receiving the bitcoins")
parser.add_argument('owner', help="Entity name or id sending the bitcoins")
parser.add_argument('--host', default="0.0.0.0", help='Host')
parser.add_argument('--port', default=7687, help='Port', type=int)
parser.add_argument('-u', '--username', required=True, help='User')
parser.add_argument('-p', '--password', required=True, help='Password')
parser.add_argument('--min-amount-dollars', required=False, default=1000, type=int)

if __name__ == "__main__":
    args = parser.parse_args()
    driver = GraphDatabase.driver(f"bolt://{args.host}:{args.port}",
                                  auth=(args.username, args.password),
                                  connection_timeout=3600)

    main(driver, args.receiver, args.owner,  args.min_amount_dollars)
