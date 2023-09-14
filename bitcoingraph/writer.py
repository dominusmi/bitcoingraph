import csv
import os

from bitcoingraph.model import Block


class CSVDumpWriter:

    def __init__(self, output_path, separate_header=True):
        self._output_path = output_path
        self._separate_header = separate_header

        if not os.path.exists(output_path):
            os.makedirs(output_path)

        self._write_header('blocks', ['hash:ID(Block)', 'height:long', 'timestamp:long', 'difficulty:double'])
        self._write_header('transactions', ['txid:ID(Transaction)', 'coinbase:boolean'])
        self._write_header('outputs', ['txid_n:ID(Output)', 'n:long', 'value:double', 'type'])
        self._write_header('addresses', ['address:ID(Address)'])
        self._write_header('rel_block_tx', ['hash:START_ID(Block)', 'txid:END_ID(Transaction)'])
        self._write_header('rel_block_block', ['hash:START_ID(Block)', 'prevblockhash:END_ID(Block)'])
        self._write_header('rel_tx_output',
                           ['txid:START_ID(Transaction)', 'txid_n:END_ID(Output)'])
        self._write_header('rel_input', ['txid:END_ID(Transaction)', 'txid_n:START_ID(Output)'])
        self._write_header('rel_output_address',
                           ['txid_n:START_ID(Output)', 'address:END_ID(Address)'])

    def __enter__(self):
        self._blocks_file = open(self._get_path('blocks'), 'a')
        self._transactions_file = open(self._get_path('transactions'), 'a')
        self._outputs_file = open(self._get_path('outputs'), 'a')
        self._addresses_file = open(self._get_path('addresses'), 'a')
        self._rel_block_tx_file = open(self._get_path('rel_block_tx'), 'a')
        self._rel_block_block_file = open(self._get_path('rel_block_block'), 'a')
        self._rel_tx_output_file = open(self._get_path('rel_tx_output'), 'a')
        self._rel_input_file = open(self._get_path('rel_input'), 'a')
        self._rel_output_address_file = open(self._get_path('rel_output_address'), 'a')

        self._block_writer = csv.writer(self._blocks_file, lineterminator="\n")
        self._transaction_writer = csv.writer(self._transactions_file, lineterminator="\n")
        self._output_writer = csv.writer(self._outputs_file, lineterminator="\n")
        self._address_writer = csv.writer(self._addresses_file, lineterminator="\n")
        self._rel_block_tx_writer = csv.writer(self._rel_block_tx_file, lineterminator="\n")
        self._rel_block_block_writer = csv.writer(self._rel_block_block_file, lineterminator="\n")
        self._rel_tx_output_writer = csv.writer(self._rel_tx_output_file, lineterminator="\n")
        self._rel_input_writer = csv.writer(self._rel_input_file, lineterminator="\n")
        self._rel_output_address_writer = csv.writer(self._rel_output_address_file, lineterminator="\n")
        return self

    def __exit__(self, type, value, traceback):
        self._blocks_file.close()
        self._transactions_file.close()
        self._outputs_file.close()
        self._addresses_file.close()
        self._rel_block_tx_file.close()
        self._rel_tx_output_file.close()
        self._rel_input_file.close()
        self._rel_output_address_file.close()

    def _write_header(self, filename, row):
        if self._separate_header:
            filename += '_header'
        with open(self._get_path(filename), 'w') as f:
            writer = csv.writer(f)
            header = row
            writer.writerow(header)

    def _get_path(self, filename):
        return os.path.join(self._output_path, filename + '.csv')

    def write(self, block: Block):
        def a_b(a, b):
            return '{}_{}'.format(a, b)

        self._block_writer.writerow([block.hash, block.height, block.timestamp, block.difficulty])
        if block.has_previous_block():
            self._rel_block_block_writer.writerow([block.hash, block.previous_block_hash])

        for tx in block.transactions:
            self._transaction_writer.writerow([tx.txid, tx.is_coinbase()])
            self._rel_block_tx_writer.writerow([block.hash, tx.txid])
            if not tx.is_coinbase():
                for input in tx.inputs:
                    self._rel_input_writer.writerow(
                        [tx.txid,
                         a_b(input.output_reference.txid, input.output_reference.index)])
            for output in tx.outputs:
                self._output_writer.writerow([a_b(tx.txid, output.index), output.index,
                                              output.value, output.type])
                self._rel_tx_output_writer.writerow([tx.txid, a_b(tx.txid, output.index)])
                for address in output.addresses:
                    self._address_writer.writerow([address])
                    self._rel_output_address_writer.writerow([a_b(tx.txid, output.index), address])
