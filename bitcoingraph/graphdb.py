from bitcoingraph.address import upsert_generated_addresses
from bitcoingraph.blockchain import BlockchainException
from bitcoingraph.common import is_pypy
from bitcoingraph.entities import upsert_entities
from bitcoingraph.logger import get_logger

if not is_pypy():
    import neo4j

from bitcoingraph.neo4j import Neo4jController
from bitcoingraph.helper import to_time

logger = get_logger("graphdb")

def round_value(bitcoin_value):
    return round(bitcoin_value, 8)


class GraphController:

    rows_per_page_default = 20

    def __init__(self, host, port, user, password):
        self.driver = neo4j.GraphDatabase.driver(f"bolt://{host}:{port}",
                                                 auth=(user, password),
                                                 connection_timeout=3600)

        self.graph_db = Neo4jController(self.driver)

    def get_address_info(self, address, date_from=None, date_to=None,
                         rows_per_page=rows_per_page_default):
        result = self.graph_db.address_stats_query(address).single_row()
        if result['num_transactions'] == 0:
            return {'transactions': 0}
        if date_from is None and date_to is None:
            count = result['num_transactions']
        else:
            count = self.graph_db.address_count_query(address, date_from, date_to).single_result()
        entity = self.graph_db.entity_query(address).single_result()
        return {'transactions': result['num_transactions'],
                'first': to_time(result['first'], True),
                'last': to_time(result['last'], True),
                'entity': entity,
                'pages': (count + rows_per_page - 1) // rows_per_page}

    def get_received_bitcoins(self, address):
        return self.graph_db.get_received_bitcoins(address)

    def get_unspent_bitcoins(self, address):
        return self.graph_db.get_unspent_bitcoins(address)

    def get_address(self, address, page, date_from=None, date_to=None,
                    rows_per_page=rows_per_page_default):
        if rows_per_page is None:
            query = self.graph_db.address_query(address, date_from, date_to)
        else:
            query = self.graph_db.paginated_address_query(address, date_from, date_to,
                                                          page * rows_per_page, rows_per_page)
        return Address(address, self.get_identities(address), query.get())

    def incoming_addresses(self, address, date_from, date_to):
        return self.graph_db.incoming_addresses(address, date_from, date_to)

    def outgoing_addresses(self, address, date_from, date_to):
        return self.graph_db.outgoing_addresses(address, date_from, date_to)

    def transaction_relations(self, address1, address2, date_from, date_to):
        trs = self.graph_db.transaction_relations(address1, address2, date_from, date_to)
        transaction_relations = [{'txid': tr['txid'], 'in': round_value(tr['in']),
                                  'out': round_value(tr['out']),
                                  'timestamp': to_time(tr['timestamp'])}
                                 for tr in trs]
        return transaction_relations

    def get_identities(self, address):
        identities = self.graph_db.identity_query(address).single_result()
        return identities

    def get_entity(self, id, max_addresses=rows_per_page_default):
        count = self.graph_db.get_number_of_addresses_for_entity(id)
        result = self.graph_db.entity_address_query(id, max_addresses)
        entity = {'id': id, 'addresses': result.get(), 'number_of_addresses': count}
        return entity

    def search_address_by_identity_name(self, name):
        address = self.graph_db.reverse_identity_query(name).single_result()
        return address

    def add_identity(self, address, name, link, source):
        self.graph_db.identity_add_query(address, name, link, source)

    def delete_identity(self, id):
        self.graph_db.identity_delete_query(id)

    def get_max_block_height(self):
        return self.graph_db.get_max_block_height()

    def add_block(self, block):
        block_query = """
        CREATE (b:Block {hash: $hash, height: $height, timestamp: $timestamp})
        WITH b
        OPTIONAL MATCH (bprev:Block {height: $height-1})
        CALL { 
            WITH b,bprev
            WITH b, bprev WHERE bprev is not null
            CREATE (b)-[:APPENDS]->(bprev)
        }
        """

        transaction_query = """
        UNWIND $transactions as tx
        MATCH (b:Block {height: $height})
        CREATE (b)-[:CONTAINS]->(t:Transaction {txid: tx.txid, coinbase: tx.coinbase})
        """

        input_query = """
        UNWIND $inputs as input
        MATCH (o:Output {txid_n: input.txid_n})
        MATCH (t:Transaction {txid: input.txid})
        CREATE (o)-[:INPUT]->(t)
        """

        output_query = """
        UNWIND $outputs as output
        CREATE (o:Output {n: output.n, txid_n: output.txid_n, type: output.type, value: output.value})
        WITH o, output
        MATCH (t:Transaction {txid: output.txid})
        CREATE (t)-[:OUTPUT]->(o)
        """

        address_query = """
        UNWIND $addresses as address
        MERGE (a:Address {address: address.address})
        WITH a, address
        MATCH (o:Output {txid_n: address.txid_n})
        CREATE (o)-[:USES]->(a)
        """

        with self.graph_db.transaction() as db_transaction:
            # block_node_id = db_transaction.add_block(block)
            transactions = []
            inputs = []
            outputs = []
            addresses = []
            try:
                pk_addresses = set([])
                grouped_addresses_per_tx = []


                logger.info("Preparing queries")
                for index, tx in enumerate(block.transactions):
                    # tx_node_id = db_transaction.add_transaction(block_node_id, tx)
                    transactions.append({'txid': tx.txid, 'coinbase': tx.is_coinbase()})
                    grouped_addresses_per_tx.append(set())
                    if not tx.is_coinbase():
                        for input_ in tx.inputs:
                            inputs.append({
                                'txid_n': '{}_{}'.format(input_.output_reference.txid, input_.output_reference.index),
                                'txid': tx.txid
                            })

                            # db_transaction.add_input(tx_node_id, input.output_reference)
                            grouped_addresses_per_tx[index].update(input_.output_reference.addresses)

                    for output in tx.outputs:
                        output_txid_n = '{}_{}'.format(output.txid, output.index)
                        outputs.append({
                            'txid_n': output_txid_n, 'n': output.index, 'value': output.value, 'type': output.type,
                            'txid': output.txid
                        })
                        # output_node_id = db_transaction.add_output(tx_node_id, output)
                        for address in output.addresses:
                            addresses.append({
                                "address": address, "txid_n": output_txid_n
                            })
                            # db_transaction.add_address(output_node_id, address)
                            if address.startswith("pk_"):
                                pk_addresses.add(address)

                logger.info("Executing queries")
                logger.debug("Loading block")
                db_transaction.tx.run(
                    block_query, {'hash': block.hash, 'height': block.height, 'timestamp': block.timestamp}
                )
                logger.debug("Loading transactions")
                db_transaction.tx.run(transaction_query, transactions=transactions, height=block.height)
                logger.debug("Loading outputs")
                db_transaction.tx.run(output_query, outputs=outputs)
                logger.debug("Loading inputs")
                db_transaction.tx.run(input_query, inputs=inputs)
                logger.debug("Loading addresses")
                db_transaction.tx.run(address_query, addresses=addresses)

                logger.info("Adding generated addresses")
                pk_to_addresses = upsert_generated_addresses(db_transaction.tx, pk_addresses)
                logger.info("Adding entities")
                upsert_entities(db_transaction.tx, grouped_addresses_per_tx, pk_to_addresses)
                logger.info("Block completed. Committing transaction")

            except BlockchainException as e:
                if e.inner_exc and e.inner_exc.args and 'genesis' in e.inner_exc.args[0]:
                    logger.warn("Skipping inputs for genesis block")
                else:
                    raise e


class Address:

    def __init__(self, address, identities, outputs):
        self.address = address
        self.identities = identities
        self.outputs = [{'txid': o['txid'], 'value': round_value(o['value']),
                         'timestamp': to_time(o['timestamp'])}
                        for o in outputs]

    def get_incoming_transactions(self):
        for output in self.outputs:
            if output['value'] > 0:
                yield output

    def get_outgoing_transactions(self):
        for output in self.outputs:
            if output['value'] < 0:
                yield {'txid': output['txid'], 'value': -output['value'],
                       'timestamp': output['timestamp']}


class Path:

    def __init__(self, raw_path):
        self.raw_path = raw_path

    @property
    def path(self):
        if self.raw_path:
            path = []
            for idx, row in enumerate(self.raw_path):
                if 'txid' in row:
                    path.append(row)
                else:
                    output = row
                    if idx != 0:
                        path.append(output)
                    path.append({'address': row['addresses'][0]})
                    if idx != len(self.raw_path) - 1:
                        path.append(output)
            return path
        else:
            return None
