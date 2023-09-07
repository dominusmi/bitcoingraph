"""
blockchain

An API for traversing the Bitcoin blockchain

"""
import json
import os
from typing import Iterator

from bitcoingraph.model import Block, Transaction
from bitcoingraph.bitcoind import BitcoindException

__author__ = 'Bernhard Haslhofer (bernhard.haslhofer@ait.ac.at)'
__copyright__ = 'Copyright 2015, Bernhard Haslhofer'
__license__ = "MIT"


class BlockchainException(Exception):
    """
    Exception raised when accessing or navigating the block chain.
    """

    def __init__(self, msg, inner_exc):
        self.msg = msg
        self.inner_exc = inner_exc

    def __str__(self):
        return repr(self.msg)


class Blockchain:

    """
    Bitcoin block chain.
    """

    def __init__(self, bitcoin_proxy):
        """
        Creates a block chain object.

        :param BitcoinProxy bitcoin_proxy: reference to Bitcoin proxy
        :return: block chain object
        :rtype: Blockchain
        """
        self._bitcoin_proxy = bitcoin_proxy

    def get_block_by_hash(self, block_hash):
        """
        Returns a block by given block hash.

        :param str block_hash: hash of block to be returned
        :return: the requested block
        :rtype: Block
        :raises BlockchainException: if block cannot be retrieved
        """
        # Returns block by hash
        try:
            raw_block_data = self._bitcoin_proxy.getblock(block_hash)

            if os.environ.get("BC_CACHE") == "1":
                with open(f"block-{block_hash}.json", "w+") as f:
                    f.write(json.dumps(raw_block_data))

            return Block.model_validate(raw_block_data)
        except BitcoindException as exc:
            raise BlockchainException('Cannot retrieve block {}'.format(block_hash), exc)

    def get_block_by_height(self, block_height):
        """
        Returns a block by given block height.

        :param int block_height: height of block to be returned
        :return: the requested block
        :rtype: Block
        :raises BlockchainException: if block cannot be retrieved
        """
        # Returns block by height
        try:
            block_hash = self._bitcoin_proxy.getblockhash(block_height)
            return self.get_block_by_hash(block_hash)
        except BitcoindException as exc:
            raise BlockchainException(
                'Cannot retrieve block with height {}'.format(block_height), exc)

    def get_blocks_in_range(self, start_height=0, end_height=0) -> Iterator[Block]:
        """
        Generates blocks in a given range.

        :param int start_height: first block height in range
        :param int end_height: last block height in range
        :yield: the requested blocks
        :rtype: Block
        """
        block = self.get_block_by_height(start_height)
        while block.height <= end_height:
            yield block
            if block.has_next_block():
                block = self.get_block_by_hash(block.next_block_hash)
            else:
                break

    def get_transaction(self, tx_id):
        """
        Returns a transaction by given transaction id.

        :param str tx_id: transaction id
        :return: the requested transaction
        :rtype: Transaction
        """
        try:
            raw_tx_data = self._bitcoin_proxy.getrawtransaction(tx_id)
            return Transaction(self, json_data=raw_tx_data)
        except BitcoindException as exc:
            print(exc)
            raise BlockchainException('Cannot retrieve transaction with id {}'.format(tx_id), exc)

    def get_max_block_height(self):
        """
        Returns maximum known block height.

        :return: maximum block height
        :rtype: int
        """
        try:
            max_height = self._bitcoin_proxy.getblockcount()
            return max_height
        except BitcoindException as exc:
            raise BlockchainException("Error when retrieving maximum\
                block height", exc)
