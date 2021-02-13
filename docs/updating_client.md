# Updating Bitcoin Core Client

When updating Bitcoin core client, update mock rpc responses in `./tests/data`
then run all the unit tests to make sure changes in rpc API does not break bitcoingraph

## Update Transaction Data 

    time curl --data-binary '{"jsonrpc": "1.0", "id":"curltext", "method": "getrawtransaction", "params": \
    ["e9a66845e05d5abc0ad04ec80f774a7e585c6e8db975962d069a522137b80c1d", 1] }' -H 'content-type: text/plain;' \
    http://rpc_user:rpc_pass@localhost:8332/

## Update Block Data

    time curl --data-binary '{"jsonrpc": "1.0", "id":"curltext", "method": "getblock", "params":  \
    ["000000000002d01c1fccc21636b607dfd930d31d01c3a62104612a1719011250", 1] }' -H 'content-type: text/plain;' \
    http://rpc_user:rpc_pass@localhost:8332/