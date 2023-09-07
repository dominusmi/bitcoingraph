import copy
import dataclasses
import inspect

import pydantic
from pydantic import BaseModel, validator, field_validator, model_validator
from typing import Optional, List, Union, Dict
from bitcoingraph.helper import to_time


class ScriptPubKey(BaseModel):
    type: str
    address: Optional[str] = pydantic.Field(default=None)
    asm: Optional[str] = pydantic.Field(default=None)

class Output(BaseModel):
    txid: str
    value: float
    index: int = pydantic.Field(alias="n")
    script_pub_key: ScriptPubKey = pydantic.Field(alias="scriptPubKey")


    @property
    def type(self) -> str:
        return self.script_pub_key.type
    @property
    def addresses(self) -> List[str]:
        if (address := self.script_pub_key.address) is not None:
            return [address]
        # Check if scriptPubKey.type indicates P2PK transaction, we then extract the pubkey as address from asm object
        # which is in the format of '<pubkey> OP_CHECKSIG'
        elif self.type == 'pubkey':
            return ['pk_' + self.script_pub_key.asm[0:130]]
        else:
            return []

class AbstractInput(BaseModel):
    @property
    def is_coinbase(self):
        return False

class CoinbaseInput(AbstractInput):
    coinbase: str

    @property
    def is_coinbase(self):
        return True

class Input(AbstractInput):
    txid: str
    vout: int
    output_reference: Optional[Output] = pydantic.Field(None)

    @model_validator(mode="before")
    def setup_output_reference(cls, obj: Dict) -> Dict:
        new_obj = copy.deepcopy(obj)
        if prevout := obj.get('prevout', {}):
            new_obj['output_reference'] = {}
            new_obj['output_reference']['txid'] = obj['txid']
            new_obj['output_reference']['n'] = obj['vout']
            new_obj['output_reference']['value'] = prevout['value']
            new_obj['output_reference']['scriptPubKey'] = prevout['scriptPubKey']
        return new_obj

class Transaction(BaseModel):
    txid: str
    inputs: List[Union[Input, CoinbaseInput]] = pydantic.Field(alias="vin")
    outputs: List[Output] = pydantic.Field(alias="vout")

    @model_validator(mode="before")
    def setup_txid_in_vout(cls, obj: Dict) -> Dict:
        """
        We need to pass the txid along to the vout
        """
        new_obj = copy.deepcopy(obj)
        for vout in new_obj["vout"]:
            vout["txid"] = new_obj["txid"]
        return new_obj

    def is_coinbase(self):
        try:
            return self.inputs[0].is_coinbase
        except Exception:
            print(f"Couldn't load transaction {self.txid}")
            return False

class Block(BaseModel):
    hash: str
    height: int
    time: int
    difficulty: float
    tx: List[Transaction]
    previous_block_hash: Optional[str] = pydantic.Field(None, alias="previousblockhash")
    next_block_hash: Optional[str] = pydantic.Field(None, alias="nextblockhash")

    class Config:
        extra = "ignore"

    @property
    def timestamp(self):
        return self.time

    @property
    def transactions(self):
        return self.tx

    def has_previous_block(self):
        return self.previous_block_hash is not None

    def has_next_block(self):
        return self.next_block_hash is not None

    def formatted_time(self):
        return to_time(self.timestamp)



# class Block:
#
#     def __init__(self, blockchain, hash=None, height=None, json_data=None):
#         self._blockchain = blockchain
#         if json_data is None:
#             self.__hash = hash
#             self.__height = height
#             self.__timestamp = None
#             self.__has_previous_block = None
#             self.__has_next_block = None
#             self.__transactions = None
#             self.__difficulty = None
#         else:
#             self.__hash = json_data['hash']
#             self.__height = json_data['height']
#             self.__timestamp = json_data['time']
#             self.__difficulty = json_data['difficulty']
#             if 'previousblockhash' in json_data:
#                 self.__has_previous_block = True
#                 self.__previous_block = Block(blockchain, json_data['previousblockhash'],
#                                               self.height - 1)
#             else:
#                 self.__has_previous_block = False
#                 self.__previous_block = None
#             if 'nextblockhash' in json_data:
#                 self.__has_next_block = True
#                 self.__next_block = Block(blockchain, json_data['nextblockhash'], self.height + 1)
#             else:
#                 self.__has_next_block = False
#                 self.__next_block = None
#             self.__transactions = [
#                 Transaction(blockchain, self, tx) if isinstance(tx, str)
#                 else Transaction(blockchain, self, json_data=tx)
#                 for tx in json_data['tx']]
#
#     @property
#     def hash(self):
#         if self.__hash is None:
#             self._load()
#         return self.__hash
#
#     @property
#     def height(self):
#         if self.__height is None:
#             self._load()
#         return self.__height
#
#     @property
#     def timestamp(self):
#         if self.__timestamp is None:
#             self._load()
#         return self.__timestamp
#
#     @property
#     def difficulty(self):
#         if self.__difficulty is None:
#             self._load()
#         return self.__difficulty
#
#     def formatted_time(self):
#         return to_time(self.timestamp)
#
#     @property
#     def previous_block(self):
#         self.has_previous_block()
#         return self.__previous_block
#
#     def has_previous_block(self):
#         if self.__has_previous_block is None:
#             self._load()
#         return self.__has_previous_block
#
#     @property
#     def next_block(self):
#         self.has_next_block()
#         return self.__next_block
#
#     def has_next_block(self):
#         if self.__has_next_block is None:
#             self._load()
#         return self.__has_next_block
#
#     @property
#     def transactions(self):
#         if self.__transactions is None:
#             self._load()
#         return self.__transactions
#
#     def _load(self):
#         raise NotImplementedError("This function has been removed")
#         if self.__hash is None:
#             block = self._blockchain.get_block_by_height(self.__height)
#         else:
#             block = self._blockchain.get_block_by_hash(self.__hash)
#         self.__height = block.height
#         self.__hash = block.hash
#         self.__timestamp = block.timestamp
#         self.__has_previous_block = block.has_previous_block()
#         self.__previous_block = block.previous_block
#         self.__has_next_block = block.has_next_block()
#         self.__next_block = block.next_block
#         self.__transactions = block.transactions
#         self.__difficulty = block.difficulty


# class Transaction:
#
#     def __init__(self, blockchain, block=None, txid=None, json_data=None):
#         self._blockchain = blockchain
#         self.block = block
#         if json_data is None:
#             self.txid = txid
#             self.__inputs = None
#             self.__outputs = None
#         else:
#             self.txid = json_data['txid']
#             if block is None:
#                 self.block = Block(blockchain, json_data['blockhash'])
#             self.__inputs = [
#                 Input(blockchain, is_coinbase=True) if 'coinbase' in vin
#                 else Input(blockchain, vin)
#                 for vin in json_data['vin']]
#             self.__outputs = [Output(self, i, vout) for i, vout in enumerate(json_data['vout'])]
#
#     @property
#     def inputs(self):
#         if self.__inputs is None:
#             self._load()
#         return self.__inputs
#
#     @property
#     def outputs(self):
#         if self.__outputs is None:
#             self._load()
#         return self.__outputs
#
#     def _load(self):
#         transaction = self._blockchain.get_transaction(self.txid)
#         self.__inputs = transaction.inputs
#         self.__outputs = transaction.outputs
#
#     def is_coinbase(self):
#         try:
#             return self.inputs[0].is_coinbase
#         except Exception:
#             print(f"Couldn't load transaction {self.txid}")
#             return False
#
#     def input_sum(self):
#         return sum([input.output.value for input in self.inputs])
#
#     def output_sum(self):
#         return sum([output.value for output in self.outputs])
#
#
#
# class Input:
#
#     def __init__(self, blockchain, output_reference=None, is_coinbase=False):
#         self._blockchain = blockchain
#         self.output_reference = output_reference
#         self.is_coinbase = is_coinbase
#         self.__output = None
#
#     @property
#     def output(self):
#         if self.is_coinbase:
#             return None
#         if self.__output is None:
#             self._load()
#         return self.__output
#
#     def _load(self):
#         transaction = self._blockchain.get_transaction(self.output_reference['txid'])
#         self.__output = transaction.outputs[self.output_reference['vout']]

#
# class Output:
#
#     def __init__(self, transaction, index, json_data):
#         self.transaction = transaction
#         self.index = index
#         self.value = json_data['value']
#         self.type = json_data['scriptPubKey']['type']
#         # See https://github.com/btcsuite/btcd/issues/1874
#         if 'address' in json_data['scriptPubKey']:
#             self.addresses = [json_data['scriptPubKey']['address']]
#         # Check if scriptPubKey.type indicates P2PK transaction, we then extract the pubkey as address from asm object
#         # which is in the format of '<pubkey> OP_CHECKSIG'
#         elif json_data['scriptPubKey']['type'] == 'pubkey':
#             self.addresses = ['pk_' + json_data['scriptPubKey']['asm'][0:130]]
#         else:
#             self.addresses = []
