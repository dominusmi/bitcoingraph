
from bitcoingraph.helper import to_time


class Block:

    def __init__(self, blockchain, hash=None, height=None, json_data=None):
        self._blockchain = blockchain
        if json_data is None:
            self.__hash = hash
            self.__height = height
            self.__timestamp = None
            self.__has_previous_block = None
            self.__has_next_block = None
            self.__transactions = None
            self.__difficulty = None
        else:
            self.__hash = json_data['hash']
            self.__height = json_data['height']
            self.__timestamp = json_data['time']
            self.__difficulty = json_data['difficulty']
            if 'previousblockhash' in json_data:
                self.__has_previous_block = True
                self.__previous_block = Block(blockchain, json_data['previousblockhash'],
                                              self.height - 1)
            else:
                self.__has_previous_block = False
                self.__previous_block = None
            if 'nextblockhash' in json_data:
                self.__has_next_block = True
                self.__next_block = Block(blockchain, json_data['nextblockhash'], self.height + 1)
            else:
                self.__has_next_block = False
                self.__next_block = None
            self.__transactions = [
                Transaction(blockchain, self, tx) if isinstance(tx, str)
                else Transaction(blockchain, self, json_data=tx)
                for tx in json_data['tx']]

    @property
    def hash(self):
        if self.__hash is None:
            self._load()
        return self.__hash

    @property
    def height(self):
        if self.__height is None:
            self._load()
        return self.__height

    @property
    def timestamp(self):
        if self.__timestamp is None:
            self._load()
        return self.__timestamp

    @property
    def difficulty(self):
        if self.__difficulty is None:
            self._load()
        return self.__difficulty

    def formatted_time(self):
        return to_time(self.timestamp)

    @property
    def previous_block(self):
        self.has_previous_block()
        return self.__previous_block

    def has_previous_block(self):
        if self.__has_previous_block is None:
            self._load()
        return self.__has_previous_block

    @property
    def next_block(self):
        self.has_next_block()
        return self.__next_block

    def has_next_block(self):
        if self.__has_next_block is None:
            self._load()
        return self.__has_next_block

    @property
    def transactions(self):
        if self.__transactions is None:
            self._load()
        return self.__transactions

    def _load(self):
        if self.__hash is None:
            block = self._blockchain.get_block_by_height(self.__height)
        else:
            block = self._blockchain.get_block_by_hash(self.__hash)
        self.__height = block.height
        self.__hash = block.hash
        self.__timestamp = block.timestamp
        self.__has_previous_block = block.has_previous_block()
        self.__previous_block = block.previous_block
        self.__has_next_block = block.has_next_block()
        self.__next_block = block.next_block
        self.__transactions = block.transactions
        self.__difficulty = block.difficulty


class Transaction:

    def __init__(self, blockchain, block=None, txid=None, json_data=None):
        self._blockchain = blockchain
        self.block = block
        if json_data is None:
            self.txid = txid
            self.__inputs = None
            self.__outputs = None
        else:
            self.txid = json_data['txid']
            if block is None:
                self.block = Block(blockchain, json_data['blockhash'])
            self.__inputs = [
                Input(blockchain, is_coinbase=True) if 'coinbase' in vin
                else Input(blockchain, vin)
                for vin in json_data['vin']]
            self.__outputs = [Output(self, i, vout) for i, vout in enumerate(json_data['vout'])]

    @property
    def inputs(self):
        if self.__inputs is None:
            self._load()
        return self.__inputs

    @property
    def outputs(self):
        if self.__outputs is None:
            self._load()
        return self.__outputs

    def _load(self):
        transaction = self._blockchain.get_transaction(self.txid)
        self.__inputs = transaction.inputs
        self.__outputs = transaction.outputs

    def is_coinbase(self):
        return self.inputs[0].is_coinbase

    def input_sum(self):
        return sum([input.output.value for input in self.inputs])

    def output_sum(self):
        return sum([output.value for output in self.outputs])

    def aggregated_inputs(self):
        aggregated_inputs = {}
        for input in self.inputs:
            output = input.output
            if input.is_coinbase:
                aggregated_inputs['COINBASE'] = self.output_sum()
            elif output.addresses[0] in aggregated_inputs:
                aggregated_inputs[output.addresses[0]] += output.value
            else:
                aggregated_inputs[output.addresses[0]] = output.value
        return aggregated_inputs

    def aggregated_outputs(self):
        aggregated_outputs = {}
        for output in self.outputs:
            if output.addresses:
                if output.addresses[0] in aggregated_outputs:
                    aggregated_outputs[output.addresses[0]] += output.value
                else:
                    aggregated_outputs[output.addresses[0]] = output.value
        return aggregated_outputs

    @staticmethod
    def _reduced_values(values, other_values):
        reduced_values = {}
        for address, value in values.items():
            if address in other_values:
                other_value = other_values[address]
                if value > other_value:
                    reduced_values[address] = value - other_value
            else:
                reduced_values[address] = value
        return reduced_values

    def reduced_inputs(self):
        return self._reduced_values(self.aggregated_inputs(), self.aggregated_outputs())

    def reduced_outputs(self):
        return self._reduced_values(self.aggregated_outputs(), self.aggregated_inputs())


class Input:

    def __init__(self, blockchain, output_reference=None, is_coinbase=False):
        self._blockchain = blockchain
        self.output_reference = output_reference
        self.is_coinbase = is_coinbase
        self.__output = None

    @property
    def output(self):
        if self.is_coinbase:
            return None
        if self.__output is None:
            self._load()
        return self.__output

    def _load(self):
        transaction = self._blockchain.get_transaction(self.output_reference['txid'])
        self.__output = transaction.outputs[self.output_reference['vout']]


class Output:

    def __init__(self, transaction, index, json_data):
        self.transaction = transaction
        self.index = index
        self.value = json_data['value']
        self.type = json_data['scriptPubKey']['type']
        if 'addresses' in json_data['scriptPubKey']:
            self.addresses = json_data['scriptPubKey']['addresses']
        # Check if scriptPubKey.type indicates P2PK transaction, we then extract the pubkey as address from asm object
        # which is in the format of '<pubkey> OP_CHECKSIG'
        elif json_data['scriptPubKey']['type'] == 'pubkey':
            self.addresses = ['pk_' + json_data['scriptPubKey']['asm'][0:130]]
        else:
            self.addresses = []
