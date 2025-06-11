"""
bitcoingraph

A Python library for extracting and navigating graph structures from
the Bitcoin block chain.

"""
import os
import shutil
import subprocess
from pathlib import Path

from bitcoingraph.logger import get_logger

import tqdm

from bitcoingraph.bitcoind import BitcoinProxy, BitcoindException
from bitcoingraph.blockchain import Blockchain
from bitcoingraph import entities
from bitcoingraph.graphdb import GraphController
from bitcoingraph.helper import sort
from bitcoingraph.writer import CSVDumpWriter

logger = get_logger('bitcoingraph')


class BitcoingraphException(Exception):
    """
    Top-level exception raised when interacting with bitcoingraph
    library.
    """

    def __init__(self, msg, inner_exc):
        self.msg = msg
        self.inner_exc = inner_exc

    def __str__(self):
        return self.msg


class BitcoinGraph:
    """Facade which provides the main access to this package."""

    def __init__(self, **config):
        """Create an instance based on the configuration."""
        self._blockchain = None
        self.blockchain_config = config['blockchain']
        if 'neo4j' in config:
            nc = config['neo4j']
            self.graph_db = GraphController(nc['host'], nc['port'], nc['user'], nc['pass'])

    @property
    def blockchain(self):
        if self._blockchain is None:
            self._blockchain = self.__get_blockchain(self.blockchain_config)
        return self._blockchain

    @staticmethod
    def __get_blockchain(config):
        """Connect to Bitcoin Core (via JSON-RPC) and return a
        Blockchain object.
        """
        try:
            print("Connecting to Bitcoin Core at {}".format(config['host']))
            bc_proxy = BitcoinProxy(**config)
            bc_proxy.getinfo()
            print("Connection successful.")
            blockchain = Blockchain(bc_proxy)
            return blockchain
        except BitcoindException as exc:
            raise BitcoingraphException("Couldn't connect to {}.".format(config['host']), exc)

    def get_transaction(self, tx_id):
        """Return a transaction."""
        return self.blockchain.get_transaction(tx_id)

    def incoming_addresses(self, address, date_from, date_to):
        return self.graph_db.incoming_addresses(address, date_from, date_to)

    def outgoing_addresses(self, address, date_from, date_to):
        return self.graph_db.outgoing_addresses(address, date_from, date_to)

    def transaction_relations(self, address1, address2, date_from=None, date_to=None):
        return self.graph_db.transaction_relations(address1, address2, date_from, date_to)

    def get_block_by_height(self, height):
        """Return the block for a given height."""
        return self.blockchain.get_block_by_height(height)

    def get_block_by_hash(self, hash):
        """Return a block."""
        return self.blockchain.get_block_by_hash(hash)

    def search_address_by_identity_name(self, term):
        """Return an address that has an associated identity with
        the given name.
        """
        return self.graph_db.search_address_by_identity_name(term)

    def get_address_info(self, address, date_from, date_to):
        """Return basic address information for the given
        time period.
        """
        return self.graph_db.get_address_info(address, date_from, date_to)

    def get_address(self, address, current_page, date_from, date_to,
                    rows_per_page=GraphController.rows_per_page_default):
        """Return an address with its transaction uses in a given
        time period.
        """
        return self.graph_db.get_address(address, current_page, date_from, date_to, rows_per_page)

    def get_identities(self, address):
        """Return a list of identities."""
        return self.graph_db.get_identities(address)

    def add_identity(self, address, name, link, source):
        """Add an identity to an address."""
        self.graph_db.add_identity(address, name, link, source)

    def delete_identity(self, identity_id):
        """Delete an identity."""
        return self.graph_db.delete_identity(identity_id)

    def get_entity(self, id):
        """Return an entity."""
        return self.graph_db.get_entity(id)

    def get_path(self, start, end):
        """Return a path between addresses."""
        raise NotImplemented("This function was removed in new version")

    def get_received_bitcoins(self, address):
        """Return the total number of bitcoins received by this address."""
        return self.graph_db.get_received_bitcoins(address)

    def get_unspent_bitcoins(self, address):
        """Return the current balance of this address."""
        return self.graph_db.get_unspent_bitcoins(address)

    @staticmethod
    def append_csv(output_path: Path, updates_path: Path):
        print("Appending new CSVs to previous")
        for base_name in ['addresses.csv', 'blocks.csv', 'outputs.csv', 'transactions.csv', 'rel_block_block.csv.csv',
                          'rel_block_tx.csv.csv', 'rel_input.csv', 'rel_output_address.csv', 'rel_tx_output.csv']:
            receiving_path = output_path.joinpath(base_name)
            sending_path = updates_path.joinpath(base_name)
            subprocess.run(f"cat {str(sending_path.expanduser().resolve().absolute())} >> {str(receiving_path.expanduser().resolve().absolute())}", shell=True)

    @staticmethod
    def sort(output_path):
        print("\nWriting blocks finished. Running sorts. This will take a long time.")
        for base_name in ['addresses', 'transactions', 'rel_tx_output', 'outputs', 'rel_output_address']:
            print(f"Sorting {base_name}.csv")
            sort(output_path, base_name + '.csv', '-u')

    def resume_export(self, end, output_path, progress=None, resume=False):
        assert output_path is not None, "When using resume, output path must be provided."

        # Get latest block exported
        output_path = Path(output_path)
        assert output_path.joinpath("blocks.csv").exists(), "When using resume, the blocks.csv must exist"
        with open(output_path.joinpath("blocks.csv")) as f:
            for line in f.readlines():
                pass
            start = int(line.split(",")[1]) + 1

        # setup outpath as {outpath}/resume
        assert output_path.exists(), "When using resume, output path must exist."
        base_path = Path(output_path)

        output_path = output_path.joinpath("resume")
        if output_path.exists():
            shutil.rmtree(str(output_path.resolve().absolute()))
        output_path.mkdir(exist_ok=True)

        # actually start resume
        try:
            number_of_blocks = end - start + 1
            with CSVDumpWriter(output_path) as writer:
                for block in tqdm.tqdm(self.blockchain.get_blocks_in_range(start, end), total=end - start):
                    writer.write(block)
                    if progress:
                        processed_blocks = block.height - start + 1
                        last_percentage = ((processed_blocks - 1) * 100) // number_of_blocks
                        percentage = (processed_blocks * 100) // number_of_blocks
                        if percentage > last_percentage:
                            progress(processed_blocks / number_of_blocks)
        except KeyboardInterrupt as e:
            answer = input("Save progress? [y/n]").strip().lower()
            if answer != "y":
                print("Exited without saving")
                raise e

        # merge files together
        self.append_csv(base_path, output_path)

        # sort them again
        self.sort(base_path)

    def export(self, start, end, output_path=None, progress=None, sort_only=False, resume=False):
        """Export the blockchain into CSV files."""
        if resume:
            self.resume_export(end, output_path, progress)
            return

        if not sort_only:
            if output_path is None:
                output_path = 'blocks_{}_{}'.format(start, end)

            number_of_blocks = end - start + 1
            with CSVDumpWriter(output_path) as writer:
                for block in tqdm.tqdm(self.blockchain.get_blocks_in_range(start, end), total=end - start):
                    writer.write(block)
                    if progress:
                        processed_blocks = block.height - start + 1
                        last_percentage = ((processed_blocks - 1) * 100) // number_of_blocks
                        percentage = (processed_blocks * 100) // number_of_blocks
                        if percentage > last_percentage:
                            progress(processed_blocks / number_of_blocks)

        self.sort(output_path)

    def synchronize(self, max_height=None, lag=0):
        """Synchronise the graph database with the blockchain
        information from the bitcoin client.
        """
        max_block_height = self.graph_db.get_max_block_height()
        if max_block_height is None:
            start = 0
        else:
            start = self.graph_db.get_max_block_height() + 1
        blockchain_end = self.blockchain.get_max_block_height() - lag
        if start > blockchain_end:
            return
        else:
            if max_height is None:
                end = blockchain_end
            else:
                end = min(max_height, blockchain_end)
            if start >= end:
                logger.warn(f"Start ({start}) >= end ({end}). Exiting.")
                raise StopIteration

            logger.info(f"Getting blocks in range {start}-{end}")
            for block in self.blockchain.get_blocks_in_range(start, end):
                if block.height >= self.blockchain.get_max_block_height() - lag:
                    return
                logger.info("Adding block")
                self.graph_db.add_block(block)
                yield block.height
                if block.height >= max_height:
                    logger.info("Reached max height. Exiting")
                    raise StopIteration


def compute_entities(input_path, sort_input=True, sort_output_address=False):
    """
    Read exported CSV files containing blockchain information and
    export entities into CSV files.
    """
    if sort_output_address:
        print("Sorting rel_output_address.csv")
        sort(input_path, 'rel_output_address.csv')

    if sort_input:
        print("Sorting rel_input.csv")
        sort(input_path, 'rel_input.csv', '-k 2 -t ,')
        entities.calculate_input_addresses(input_path)
        print("Sorting input_addresses.csv")
        sort(input_path, 'input_addresses.csv')

    print("Computing entities")
    entities.compute_entities(input_path)
