import os
import unittest

from bitcoingraph.blockchain import Blockchain
from bitcoingraph.graphdb import GraphController
from tests.rpc_mock import BitcoinProxyMock

BH1 = "000000000002d01c1fccc21636b607dfd930d31d01c3a62104612a1719011250"
BH2 = "000000000003ba27aa200b1cecaad478d2b00432346c3f1f3986da1afd33e506"
BH3 = "00000000000080b66c911bd5ba14a74260057311eaeb1982802f7010f1a9f090"

class TestBlockchainObject(unittest.TestCase):

    def setUp(self):
        self.bitcoin_proxy = BitcoinProxyMock()
        self.blockchain = Blockchain(self.bitcoin_proxy)
        self.graph_db = GraphController(
            os.environ.get("NEO4J_HOST", "127.0.0.1"),
            os.environ.get("NEO4J_PORT", "7687"),
            os.environ.get("NEO4J_USER", "neo4j"),
            os.environ.get("NEO4J_PASSWORD", "neo4j"),
        )
        self.initialise()

    def initialise(self):
        self.assertIsNotNone(self.blockchain)
        self.assertIsNotNone(self.bitcoin_proxy)
        with self.graph_db.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")

            session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (a:Address) REQUIRE a.address IS UNIQUE;")
            session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (b:Block) REQUIRE b.height IS UNIQUE;")
            session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (o:Output) REQUIRE o.txid_n IS UNIQUE;")
            session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (t:Transaction) REQUIRE t.txid IS UNIQUE;")
            session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (e:Entity) REQUIRE e.entity_id IS UNIQUE;")

            session.run("CALL db.awaitIndexes(120)")

            for i in range(99999, 100002):
                block = self.blockchain.get_block_by_height(i)
                self.graph_db.add_block(block)

class TestLoading(TestBlockchainObject):
    def test_blocks(self):
        with self.graph_db.driver.session() as session:
            blocks = session.run("MATCH (b:Block) RETURN b ORDER BY b.height ASC").data("b")
            self.assertEqual(len(blocks), 3)
            self.assertEqual(blocks[0]['b']['hash'], BH1)
            self.assertEqual(blocks[1]['b']['hash'], BH2)
            self.assertEqual(blocks[2]['b']['hash'], BH3)

            blocks = session.run("MATCH (b:Block)-[:APPENDS]->(prevB:Block) RETURN b ORDER BY b.height ASC").data("b")
            self.assertEqual(len(blocks), 2)
            self.assertEqual(blocks[0]['b']['height'], 100000)
            self.assertEqual(blocks[1]['b']['height'], 100001)


    def test_transactions_global(self):
        with self.graph_db.driver.session() as session:
            result = session.run("MATCH (b:Block)-[:CONTAINS]->(t:Transaction) RETURN b, collect(t) as txs ORDER BY b.height ASC")

            rows = result.data()
            self.assertEqual(len(rows[0]['txs']), 1)
            self.assertEqual(len(rows[1]['txs']), 4)
            self.assertEqual(len(rows[2]['txs']), 12)

            ct1 = session.run("MATCH (t:Transaction) RETURN count(t) as ct").value("ct")[0]
            ct2 = session.run("MATCH (t:Transaction) WHERE t.txid is not null RETURN count(t) as ct").value("ct")[0]
            self.assertEqual(ct1, 17)
            self.assertEqual(ct1, ct2)


    def test_transaction_input_output_same_block(self):
        with self.graph_db.driver.session() as session:
            result = session.run("""
            MATCH (b:Block)-[:CONTAINS]->(t:Transaction)-[:OUTPUT]->(o:Output)-[:INPUT]->(t1:Transaction)<-[:CONTAINS]-(b) 
            RETURN b,t,o,t1
            """).data()

            self.assertEqual(len(result), 1)
            result = result[0]
            self.assertEqual(result['t']['txid'], "bb28a1a5b3a02e7657a81c38355d56c6f05e80b9219432e3352ddcfc3cb6304c")
            self.assertEqual(result['t1']['txid'], "fbde5d03b027d2b9ba4cf5d4fecab9a99864df2637b25ea4cbcb1796ff6550ca")

    def test_generated_addresses(self):
        with self.graph_db.driver.session() as session:
            result = session.run("""
                MATCH (a:Address)
                WHERE a.address STARTS WITH "pk"
                WITH a
                OPTIONAL MATCH (a)-[:GENERATES]->(generated)
                RETURN a, collect(generated) as generated
            """).data()

            self.assertEqual(len(result), 4)
            for row in result:
                self.assertIsNotNone(row['generated'])
                self.assertEqual(len(row['generated']), 2)


    def test_entities(self):
        with self.graph_db.driver.session() as session:
            result = session.run("MATCH (e:Entity) WITH e OPTIONAL MATCH (e)-[:OWNER_OF]->(a) RETURN e, collect(a) as owned ORDER BY e.entity_id ASC").data()
            self.assertEqual(len(result), 4)
