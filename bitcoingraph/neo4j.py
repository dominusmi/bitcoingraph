
import json

import neo4j
import requests
from datetime import date, datetime, timezone


def lb_join(*lines):
    return '\n'.join(lines)


class Neo4jException(Exception):

    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return repr(self.msg)


class Neo4jController:

    def __init__(self, driver: neo4j.Driver):
        self.driver = driver


    address_match = lb_join(
        'MATCH (a:Address {address: {address}})<-[:USES]-(o),',
        '  (o)-[r:INPUT|OUTPUT]-(t)<-[:CONTAINS]-(b)',
        'WITH a, t, b,',
        'CASE type(r) WHEN "OUTPUT" THEN sum(o.value) ELSE -sum(o.value) END AS value')
    reduced_address_match = lb_join(
        address_match,
        'WITH a, t, b, sum(value) AS value')
    address_period_match = lb_join(
        reduced_address_match,
        'WHERE b.timestamp > {from} AND b.timestamp < {to}')
    address_statement = lb_join(
        address_period_match,
        'RETURN t.txid as txid, value, b.timestamp as timestamp',
        'ORDER BY b.timestamp desc')

    def address_stats_query(self, address):
        s = lb_join(
            self.reduced_address_match,
            'RETURN count(*) as num_transactions, '
            'min(b.timestamp) as first, max(b.timestamp) as last')
        return self.query(s, {'address': address})

    def get_received_bitcoins(self, address):
        s = lb_join(
            self.reduced_address_match,
            'WHERE value > 0',
            'RETURN sum(value)')
        return self.query(s, {'address': address}).single_result()

    def get_unspent_bitcoins(self, address):
        s = lb_join(
            'MATCH (a:Address {address: {address}})<-[:USES]-(o)',
            'WHERE NOT (o)-[:INPUT]->()',
            'RETURN sum(o.value)')
        return self.query(s, {'address': address}).single_result()

    def address_count_query(self, address, date_from, date_to):
        s = lb_join(
            self.address_period_match,
            'RETURN count(*)')
        return self.query(s, self.as_address_query_parameter(address, date_from, date_to))

    def address_query(self, address, date_from, date_to):
        return self.query(self.address_statement,
                          self.as_address_query_parameter(address, date_from, date_to))

    def paginated_address_query(self, address, date_from, date_to, skip, limit):
        s = lb_join(
            self.address_statement,
            'SKIP {skip} LIMIT {limit}')
        p = self.as_address_query_parameter(address, date_from, date_to)
        p['skip'] = skip
        p['limit'] = limit
        return self.query(s, p)

    def incoming_addresses(self, address, date_from, date_to):
        return self._related_addresses(address, date_from, date_to, '<-[:OUTPUT]-(t)<-[:INPUT]-')

    def outgoing_addresses(self, address, date_from, date_to):
        return self._related_addresses(address, date_from, date_to, '-[:INPUT]->(t)-[:OUTPUT]->')

    def _related_addresses(self, address, date_from, date_to, output_relation):
        s = lb_join(
            'MATCH (a:Address {address: {address}})<-[:USES]-(o),',
            '  (o)' + output_relation + '(o2)-[:USES]->(a2),',
            '  (t)<-[:CONTAINS]-(b)',
            'WITH DISTINCT a, a2, t, b',
            'WHERE a2 <> a',
            'AND b.timestamp > {from} AND b.timestamp < {to}',
            'RETURN a2.address as address, count(t) as transactions',
            'ORDER BY transactions desc')
        return self.query(s, self.as_address_query_parameter(address, date_from, date_to)).get()

    def transaction_relations(self, address, address2, date_from, date_to):
        s = lb_join(
            'MATCH (a:Address {address: {address}})<-[:USES]-(o),',
            '  (o)-[:INPUT]->(t)-[:OUTPUT]->(o2),',
            '  (o2)-[:USES]->(a2:Address {address: {address2}}),',
            '  (t)<-[:CONTAINS]-(b)',
            'WHERE b.timestamp > {from} AND b.timestamp < {to}',
            'WITH a, a2, t, b, collect(DISTINCT o) as ins, collect(DISTINCT o2) as outs',
            'RETURN t.txid as txid, reduce(sum=0, o in ins | sum+o.value) as in,',
            '  reduce(sum=0, o in outs | sum+o.value) as out, b.timestamp as timestamp',
            'ORDER BY b.timestamp desc')
        p = self.as_address_query_parameter(address, date_from, date_to)
        p['address2'] = address2
        return self.query(s, p).get()

    def entity_query(self, address):
        s = lb_join(
            'MATCH (a:Address {address: {address}})-[:BELONGS_TO]->(e)',
            'RETURN {id: id(e)}')
        return self.query(s, {'address': address})

    def get_number_of_addresses_for_entity(self, id):
        s = lb_join(
            'MATCH (e:Entity)',
            'WHERE id(e) = $id',
            'RETURN size((e)<-[:BELONGS_TO]-())')
        return self.query(s, {'id': id}).single_result()

    def entity_address_query(self, id, limit):
        s = lb_join(
            'MATCH (e:Entity)<-[:BELONGS_TO]-(a)',
            'WHERE id(e) = $id',
            'OPTIONAL MATCH (a)-[:HAS]->(i)',
            'WITH e, a, collect(i) as is',
            'ORDER BY length(is) desc',
            'LIMIT {limit}',
            'RETURN a.address as address, is as identities')
        return self.query(s, {'id': id, 'limit': limit})

    def identity_query(self, address):
        s = lb_join(
            'MATCH (a:Address {address: {address}})-[:HAS]->(i)',
            'RETURN collect({id: id(i), name: i.name, link: i.link, source: i.source})')
        return self.query(s, {'address': address})

    def reverse_identity_query(self, name):
        s = lb_join(
            'MATCH (i:Identity {name: {name}})<-[:HAS]-(a)',
            'RETURN a.address')
        return self.query(s, {'name': name})

    def identity_add_query(self, address, name, link, source):
        s = lb_join(
            'MATCH (a:Address {address: {address}})',
            'CREATE (a)-[:HAS]->(i:Identity {name: {name}, link: {link}, source: {source}})')
        return self.query(s, {'address': address, 'name': name, 'link': link, 'source': source})

    def identity_delete_query(self, id):
        s = lb_join(
            'MATCH (i:Identity)',
            'WHERE id(i) = $id',
            'DETACH DELETE i')
        return self.query(s, {'id': id})

    def path_query_old(self, address1, address2):
        s = lb_join(
            'MATCH (start:Address {address: $address1})<-[:USES]-(o1:Output)',
            '  -[:INPUT|OUTPUT*]->(o2:Output)-[:USES]->(end:Address {address: $address2}),',
            '  p = shortestpath((o1)-[:INPUT|OUTPUT*]->(o2))',
            'WITH p',
            'LIMIT 1',
            'UNWIND nodes(p) as n',
            'OPTIONAL MATCH (n)-[:USES]->(a)',
            'RETURN n as node, a as address')
        return self.query(s, {'address1': address1, 'address2': address2})

    def get_id_of_address_node(self, address):
        s = lb_join(
            'MATCH (a:Address {address: $address})',
            'RETURN id(a)')
        return self.query(s, {'address': address}).single_result()

    def get_max_block_height(self):
        s = lb_join(
            'MATCH (b:Block)',
            'RETURN max(b.height) as maxHeight')
        return self.query(s).single_result()["maxHeight"]

    def add_block(self, block):
        s = lb_join(
            'CREATE (b:Block {hash: $hash, height: $height, timestamp: $timestamp})',
            'RETURN id(b)')
        p = {'hash': block.hash, 'height': block.height, 'timestamp': block.timestamp}
        return self.query(s, p).single_result()

    def add_transaction(self, block_node_id, tx):
        s = lb_join(
            'MATCH (b) WHERE id(b) = $id',
            'CREATE (b)-[:CONTAINS]->(t:Transaction {txid: $txid, coinbase: $coinbase})',
            'RETURN id(t)')
        p = {'id': block_node_id, 'txid': tx.txid, 'coinbase': tx.is_coinbase()}
        return self.query(s, p).single_result()

    def add_input(self, tx_node_id, output_reference):
        s = lb_join(
            'MATCH (o:Output {txid_n: $txid_n}), (t)',
            'WHERE id(t) = $id',
            'CREATE (o)-[:INPUT]->(t)')
        p = {'txid_n': '{}_{}'.format(output_reference['txid'], output_reference['vout']),
             'id': tx_node_id}
        return self.query(s, p).single_result()

    def add_output(self, tx_node_id, output):
        s = lb_join(
            'MATCH (t) WHERE id(t) = $id',
            'CREATE (t)-[:OUTPUT]->'
            '(o:Output {txid_n: $txid_n, n: $n, value: $value, type: $type})',
            'RETURN id(o)')
        p = {'id': tx_node_id, 'txid_n': '{}_{}'.format(output.transaction.txid, output.index),
             'n': output.index, 'value': output.value, 'type': output.type}
        return self.query(s, p).single_result()

    def add_address(self, output_node_id, address):
        s = lb_join(
            'MATCH (o) WHERE id(o) = $id',
            'MERGE (a:Address {address: $address})',
            'CREATE (o)-[:USES]->(a)',
            'RETURN id(a)')
        return self.query(s, {'id': output_node_id, 'address': address}).single_result()

    def query(self, statement, parameters=None):
        if parameters is None:
            parameters = {}
        try:
            with self.driver.session() as session:
                r = session.run(statement, **parameters)
                return QueryResult(r.data())

        except Exception as e:
            raise Neo4jException(str(e))

    @staticmethod
    def as_address_query_parameter(address, date_from=None, date_to=None):
        if date_from is None:
            timestamp_from = 0
        else:
            timestamp_from = datetime.strptime(date_from, '%Y-%m-%d').replace(
                tzinfo=timezone.utc).timestamp()
        if date_to is None:
            timestamp_to = 2 ** 31 - 1
        else:
            d = datetime.strptime(date_to, '%Y-%m-%d').replace(tzinfo=timezone.utc)
            d += date.resolution
            timestamp_to = d.timestamp()
        return {'address': address, 'from': timestamp_from, 'to': timestamp_to}

    def transaction(self) -> 'DBTransaction':
        return DBTransaction(self.driver)


class DBTransaction(Neo4jController):

    def __enter__(self):
        self.session = self.driver.session()
        tx = self.session.begin_transaction()
        self.tx = tx
        return self

    def __exit__(self, type, value, traceback):
        self.tx.commit()
        self.tx.close()
        self.session.close()

    def query(self, statement, parameters=None):
        if parameters is None:
            parameters = {}
        try:
            r = self.tx.run(statement, **parameters)
            return QueryResult(r.data())

        except Exception as e:
            raise Neo4jException(str(e))


class QueryResult:

    def __init__(self, raw_data):
        self._raw_data = raw_data

    @property
    def data(self):
        return self._raw_data

    def columns(self):
        return self._raw_data['results'][0]['columns']

    def get(self):
        return [dict(zip(self.columns(), r['row'])) for r in self.data()]

    def list(self):
        return [r['row'][0] for r in self.data()]

    def single_result(self):
        if self.data:
            return self.data[0]
        else:
            return None

    def single_row(self):
        rows = self.get()
        if self.get():
            return list(self.get())[0]
        else:
            return None
