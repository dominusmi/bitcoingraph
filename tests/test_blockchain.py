import unittest

from tests.rpc_mock import BitcoinProxyMock

from bitcoingraph.blockchain import Blockchain, BlockchainException
from bitcoingraph.model import Input, Output, CoinbaseInput

BH1 = "000000000002d01c1fccc21636b607dfd930d31d01c3a62104612a1719011250"
BH1_HEIGHT = 99999
BH2 = "000000000003ba27aa200b1cecaad478d2b00432346c3f1f3986da1afd33e506"
BH2_HEIGHT = 100000
BH3 = "00000000000080b66c911bd5ba14a74260057311eaeb1982802f7010f1a9f090"
BH3_HEIGHT = 100001

# standard transactions
TX1 = "8c14f0db3df150123e6f3dbbf30f8b955a8249b62ac1d1ff16284aefa3d06d87"
TX2 = "fff2525b8931402dd09222c50775608f75787bd2b87e56995a7bdd30f79702c4"
TX3 = "87a157f3fd88ac7907c05fc55e271dc4acdc5605d187d646604ca8c0e9382e03"

# transaction with unknown output
TXE = "a288fec5559c3f73fd3d93db8e8460562ebfe2fcf04a5114e8d0f2920a6270dc"

# transaction with multiple in and outputs
TXM = "d5f013abf2cf4af6d68bcacd675c91f19bab5b7103b4ac2f4941686eb47da1f0"


class TestBlockchainObject(unittest.TestCase):

    def setUp(self):
        self.bitcoin_proxy = BitcoinProxyMock()
        self.blockchain = Blockchain(self.bitcoin_proxy)

    def test_init(self):
        self.assertIsNotNone(self.blockchain)
        self.assertIsNotNone(self.bitcoin_proxy)


class TestBlock(TestBlockchainObject):

    def test_time(self):
        block = self.blockchain.get_block_by_hash(BH1)
        self.assertEqual(block.timestamp, 1293623731)

    def test_time_as_dt(self):
        block = self.blockchain.get_block_by_hash(BH1)
        self.assertEqual(block.formatted_time(), "2010-12-29 11:55:31")

    def test_height(self):
        block = self.blockchain.get_block_by_hash(BH1)
        self.assertEqual(block.height, BH1_HEIGHT)

    def test_hash(self):
        block = self.blockchain.get_block_by_hash(BH1)
        self.assertEqual(block.hash, BH1)

    def test_nextblockhash(self):
        block = self.blockchain.get_block_by_hash(BH1)
        self.assertTrue(block.has_next_block())
        block = self.blockchain.get_block_by_hash(BH3)
        self.assertFalse(block.has_next_block())

    def test_tx_count(self):
        block = self.blockchain.get_block_by_hash(BH1)
        self.assertEqual(len(block.transactions), 1)
        block = self.blockchain.get_block_by_hash(BH2)
        self.assertEqual(len(block.transactions), 4)

    def test_tx_ids(self):
        block = self.blockchain.get_block_by_hash(BH2)
        self.assertTrue(TX1 in [transaction.txid for transaction in block.transactions])

    def test_transactions(self):
        block = self.blockchain.get_block_by_hash(BH1)
        txs = [tx for tx in block.transactions]
        self.assertEqual(len(txs), 1)
        for tx in txs:
            self.assertIsNotNone(tx.txid)
        block = self.blockchain.get_block_by_hash(BH2)
        txs = [tx for tx in block.transactions]
        self.assertEqual(len(txs), 4)
        for tx in txs:
            self.assertIsNotNone(tx.txid)

    def test_difficulty(self):
        block = self.blockchain.get_block_by_hash(BH1)
        self.assertAlmostEqual(block.difficulty, 14484.1623612254)

    def test_prev_hash(self):
        block = self.blockchain.get_block_by_hash(BH1)
        self.assertEqual(block.previous_block_hash, "0000000000002103637910d267190996687fb095880d432c6531a527c8ec53d1")


class TestTxInput(TestBlockchainObject):

    def test_is_coinbase(self):
        block = self.blockchain.get_block_by_hash(BH1)
        tx = block.transactions[0]
        tx_input = tx.inputs[0]
        self.assertTrue(tx_input.is_coinbase)

    def test_is_not_coinbase(self):
        block = self.blockchain.get_block_by_hash(BH2)
        tx = block.transactions[1]
        tx_input = tx.inputs[0]
        self.assertFalse(tx_input.is_coinbase)

    def test_prev_tx_hash(self):
        block = self.blockchain.get_block_by_hash(BH2)
        tx = block.transactions[1]
        tx_input = tx.inputs[0]
        self.assertEqual(tx_input.output_reference.txid, "87a157f3fd88ac7907c05fc55e271dc4acdc5605d187d646604ca8c0e9382e03")

    def test_tx_output_index(self):
        block = self.blockchain.get_block_by_hash(BH2)
        tx = block.transactions[1]
        tx_input = tx.inputs[0]
        self.assertEqual(tx_input.output_reference.index, 0)

    def test_addresses(self):
        block = self.blockchain.get_block_by_hash(BH2)
        tx = block.transactions[1]
        tx_input = tx.inputs[0]
        self.assertEqual(len(tx_input.output_reference.addresses), 1)
        self.assertEqual(
            tx_input.output_reference.addresses[0],
            "1BNwxHGaFbeUBitpjy2AsKpJ29Ybxntqvb",
        )


class TestTxOutput(TestBlockchainObject):
    def get_tx(self):
        block = self.blockchain.get_block_by_hash(BH1)
        return block.transactions[0]

    def test_index(self):
        block = self.blockchain.get_block_by_hash(BH2)
        tx = block.transactions[1]
        self.assertEqual(0, tx.outputs[0].index)
        self.assertEqual(1, tx.outputs[1].index)

    def test_value(self):
        block = self.blockchain.get_block_by_hash(BH2)
        tx = block.transactions[1]
        self.assertEqual(5.56000000, tx.outputs[0].value)
        self.assertEqual(44.44000000, tx.outputs[1].value)

    def test_addresses(self):
        block = self.blockchain.get_block_by_hash(BH2)
        tx = block.transactions[1]
        self.assertEqual("1JqDybm2nWTENrHvMyafbSXXtTk5Uv5QAn",
                         tx.outputs[0].addresses[0])
        self.assertEqual("1EYTGtG4LnFfiMvjJdsU7GMGCQvsRSjYhx",
                         tx.outputs[1].addresses[0])

    def test_empty_addresses(self):
        """
        Test if empty list is return when no output addresses are present.
        """
        block = self.blockchain.get_block_by_hash(BH2)
        tx = block.transactions[3]
        self.assertEqual(["pk_0469b7eaf1cca8a7c8592ad49313b4cb6474a845604456d48b4b252904e1d61ceda95ac987ad163e957bdbd2da2736861fbfad93dbf8e0a218308a49d94ab9a077"],
                         tx.outputs[0].addresses)
        self.assertFalse(tx.outputs[1].addresses)


class TestTransaction(TestBlockchainObject):

    def test_blocktime(self):
        block = self.blockchain.get_block_by_hash(BH2)
        self.assertEqual(block.timestamp, 1293623863)

    def test_blocktime_as_dt(self):
        block = self.blockchain.get_block_by_hash(BH2)
        self.assertEqual(block.formatted_time(), "2010-12-29 11:57:43")

    def test_id(self):
        block = self.blockchain.get_block_by_hash(BH2)
        tx = block.transactions[0]
        self.assertEqual(tx.txid, TX1)

    def test_get_input_count(self):
        block = self.blockchain.get_block_by_hash(BH2)
        tx = block.transactions[0]
        self.assertEqual(len(tx.inputs), 1)
        tx = block.transactions[1]
        self.assertEqual(len(tx.inputs), 1)

    def test_get_inputs(self):
        block = self.blockchain.get_block_by_hash(BH2)
        tx = block.transactions[0]
        for tx_input in tx.inputs:
            self.assertIsInstance(tx_input, CoinbaseInput)

    def test_is_coinbase_tx(self):
        block = self.blockchain.get_block_by_hash(BH2)
        tx1 = block.transactions[0]
        tx2 = block.transactions[1]
        self.assertTrue(tx1.is_coinbase())
        self.assertFalse(tx2.is_coinbase())

    def test_get_output_count(self):
        block = self.blockchain.get_block_by_hash(BH2)
        tx = block.transactions[0]
        self.assertEqual(len(tx.outputs), 1)
        tx = block.transactions[1]
        self.assertEqual(len(tx.outputs), 2)

    def test_get_outputs(self):
        block = self.blockchain.get_block_by_hash(BH2)
        tx = block.transactions[0]
        for tx_output in tx.outputs:
            self.assertIsInstance(tx_output, Output)


class TestBlockchain(TestBlockchainObject):

    def test_get_block_by_hash(self):
        block = self.blockchain.get_block_by_hash(BH1)
        self.assertEqual(block.hash, BH1)

    def test_get_block_by_height(self):
        block = self.blockchain.get_block_by_height(BH1_HEIGHT)
        self.assertEqual(block.height, BH1_HEIGHT)

    def test_get_blocks_in_range(self):
        blocks = [block for block in self.blockchain.get_blocks_in_range(
                  99999, 100001)]
        self.assertEqual(len(blocks), 3)
        self.assertEqual(blocks[0].height, 99999)
        self.assertEqual(blocks[1].height, 100000)
        self.assertEqual(blocks[2].height, 100001)

    def test_get_max_blockheight(self):
        max_height = self.blockchain.get_max_block_height()
        self.assertEqual(max_height, 100001)

    def test_exceptions(self):
        with self.assertRaises(BlockchainException) as cm:
            self.blockchain.get_block_by_hash("aa")
        self.assertEqual("Cannot retrieve block aa", cm.exception.msg)

        with self.assertRaises(BlockchainException) as cm:
            self.blockchain.get_block_by_height(123)
        self.assertEqual("Cannot retrieve block with height 123",
                         cm.exception.msg)

        with self.assertRaises(BlockchainException) as cm:
            self.blockchain.get_transaction("bb")
        self.assertEqual("Cannot retrieve transaction with id bb",
                         cm.exception.msg)
