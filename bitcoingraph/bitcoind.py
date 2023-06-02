"""
Bitcoin Core JSON-RPC interface.

"""
import logging
import os
from typing import Optional

try:
    import paramiko
except ImportError:
    pass

import requests
import json

import time

__author__ = 'Bernhard Haslhofer (bernhard.haslhofer@ait.ac.at)'
__copyright__ = 'Copyright 2015, Bernhard Haslhofer'
__license__ = "MIT"


class BitcoindException(Exception):
    """
    Exception raised when accessing Bitcoin Core via JSON-RPCS.
    """
    pass


class SSHTunnel:
    def __init__(self, client: paramiko.SSHClient):
        self.client = client

    @classmethod
    def from_config_file(cls, path):
        with open(path, 'r') as file:
            vars = {"port": 22, **json.load(file)}

        print("Using SSH tunnel with at {}:{}".format(vars["hostname"], vars["port"]))
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.connect(**vars)
        return cls(ssh_client)


class JSONRPCInterface:
    """
    A generic JSON-RPC interface with keep-alive session reuse.
    """

    def __init__(self, url):
        """
        Creates a generic JSON-RPC interface object.

        :param str url: URL of JSON-RPC endpoint
        :return: JSON-RPC proxy object
        :rtype: JSONRPCInterface
        """
        self._session = requests.Session()
        self._url = url
        self._headers = {'content-type': 'application/json'}

    def call(self, rpcMethod, *params):
        """
        Execute a single request against a JSON-RPC interface
        """
        request = {"jsonrpc": "2.0",
                   "method": rpcMethod,
                   "params": list(params)}
        responseJSON = self._execute(request)
        return responseJSON['result']

    def batch(self, calls):
        """
        Executes a batch request (with same method) but different parameters
        against a JSON-RPC interface
        """
        requests = []
        for call in calls:
            request = {"jsonrpc": "2.0",
                       "method": call['method'],
                       "params": call['params'],
                       "id": call['id']}
            requests.append(request)
        responseJSON = self._execute(requests)
        return responseJSON

    def _execute(self, request):
        payload = json.dumps(request)

        tries = 5
        hadConnectionFailures = False
        while True:
            try:
                response = self._session.post(self._url, headers=self._headers,
                                              data=payload)
            except requests.exceptions.ConnectionError as e:
                print(self._url)
                print(e)
                tries -= 1
                if tries == 0:
                    raise BitcoindException('Failed to connect for RPC call.')
                hadConnectionFailures = True
                print("Couldn't connect for remote procedure call.",
                      "will sleep for ten seconds and then try again...")
                time.sleep(10)
            else:
                if hadConnectionFailures:
                    print("Connected for RPC call after retry.")
                break
        if response.status_code not in (200, 500):
            raise BitcoindException("RPC connection failure: " +
                                    str(response.status_code) + ' ' +
                                    response.reason + ' ' + response.text)
        responseJSON = response.json()
        if 'error' in responseJSON and responseJSON['error'] is not None:
            raise BitcoindException('Error in RPC call: ' +
                                    str(responseJSON['error']))
        return responseJSON


class RESTInterface:

    def __init__(self, url, ssh_tunnel: Optional[SSHTunnel] = None):
        self._session = requests.Session()
        self._url = url
        self._ssh_tunnel = ssh_tunnel

    def get_block(self, hash):
        if self._ssh_tunnel:
            curl_command = """curl http://127.0.0.1:8332/rest/block/{}.json""".format(hash)
            stdin, stdout, stderr = self._ssh_tunnel.client.exec_command(curl_command)
            output = stdout.read().decode('utf-8')
            json_output = json.loads(output)
            return json_output
        else:
            r = self._session.get(self._url + 'block/{}.json'.format(hash), timeout=120)
            if r.status_code != 200:
                raise Exception('REST request was not successful')
            return r.json()



class BitcoinProxy:
    """
    Proxy to Bitcoin JSON RPC Service.

    Implements a subset of call list described
    `here <https://en.bitcoin.it/wiki/Original_Bitcoin_client/API_Calls_list>`_
    """

    def __init__(self, host, port, rpc_user=None, rpc_pass=None, method='RPC', ssh_tunnel=None, cache_path=None):
        """
        Creates a Bitcoin JSON RPC Service object.

        :param str url: URL of Bitcoin Core JSON-RPC endpoint
        :return: bitcoin proxy object
        :rtype: BitcoinProxy
        """
        self.method = method
        rpc_url = 'http://{}:{}@{}:{}/'.format(rpc_user, rpc_pass, host, port)
        print(f'RPC URL is: {rpc_url}')
        self._jsonrpc_proxy = JSONRPCInterface(rpc_url)

        if method == 'REST':
            rest_url = 'http://{}:{}/rest/'.format(host, port)
            print(f'REST URL is: {rest_url}')
            self._rest_proxy = RESTInterface(rest_url, ssh_tunnel=ssh_tunnel)

        self._cache_path = cache_path

    def getblock(self, block_hash):
        """
        Returns information about the block with the given hash.

        :param str block_hash: the block hash
        :return: block as JSON
        :rtype: str
        """
        if self.method == 'REST':
            if self._cache_path:
                path = "{}/{}.json".format(self._cache_path, block_hash)
                if os.path.exists(path):
                    with open(path, 'r') as f:
                        r = json.load(f)
                else:
                    r = self._rest_proxy.get_block(block_hash)
                    with open(path, "w+") as f:
                        json.dump(r, f)
            else:
                r = self._rest_proxy.get_block(block_hash)

        else:
            r = self._jsonrpc_proxy.call('getblock', block_hash)
        return r

    def getblockcount(self):
        """
        Returns the number of blocks in the longest block chain.

        :return: number of blocks in block chain
        :rtype: int
        """
        r = self._jsonrpc_proxy.call('getblockcount')
        return int(r)

    def getblockhash(self, height):
        """
        Returns hash of block in best-block-chain at given height.

        :param str height: the block height
        :return: block hash
        :rtype: str
        """
        r = self._jsonrpc_proxy.call('getblockhash', height)
        return r

    def getinfo(self):
        """
        Returns an object containing various state info.

        :return: JSON string with state info
        :rtype: str
        """
        r = self._jsonrpc_proxy.call('getblockchaininfo')
        return r

    def getrawtransaction(self, tx_id, verbose=1):
        """
        Returns raw transaction representation for given transaction id.

        :param str tx_id: transaction id
        :param int verbose: complete transaction record (0 = false, 1 = true)
        :return: raw transaction data as JSON
        :rtype: str
        """
        r = self._jsonrpc_proxy.call('getrawtransaction', tx_id, verbose)
        return r

    def getrawtransactions(self, tx_ids, verbose=1):
        """
        Returns raw transaction representation for a given list of transaction
        ids.

        :param tx_ids: list of transaction ids
        :param int verbose: complete transaction record (0 = false, 1 = true)
        :return: array of raw transaction data as JSON
        :rtype: dictionary (key=id, value=result)
        """
        calls = []
        for tx_id in tx_ids:
            call = {'method': 'getrawtransaction',
                    'params': [tx_id, verbose],
                    'id': tx_id}
            calls.append(call)
        r = self._jsonrpc_proxy.batch(calls)

        results = []
        for entry in r:
            results.append(entry['result'])
        return results
