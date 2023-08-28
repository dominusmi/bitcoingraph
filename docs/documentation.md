# Table of contents
- [Neo4j pointers and examples](#neo4j-pointers)
- [Using your own bitcoin node](#own-bitcoin-node)

<a name="neo4j-pointers"></a>
## Neo4j pointers

- Before running a large query, always run the query with `EXPLAIN` first. This shows the plan of the database calls,
  and can be very useful to notice a suboptimal query
- Don't be scared of using `WITH` to aggregate results during the query, it can save a lot of time. For example
   ```cypher
   MATCH (a:Address)
   OPTIONAL MATCH (a)<-[:OWNER_OF]-(e:Entity)
   WHERE a.address in ["123","456",..]
   RETURN a,e
   ```
  looks like a good query. However, running it with explain will immediately show that the optional match actually
  matches all of the addresses (completely ignoring the `WHERE` condition). Instead, the correct use would be
   ```cypher
   MATCH (a:Address)
   WHERE a.address in ["123","456",..]
   WITH a
   OPTIONAL MATCH (a)<-[:OWNER_OF]-(e:Entity)
   RETURN a,e
   ```
- Use transactions on large queries, both for read and writes:
   ```cypher
   MATCH (a:Address)
   CALL {
     // do something with the addresses
   } IN TRANSACTION OF 1000 ROWS 
   ```

### Example queries

Get the sum of bitcoins that passed through a given address.

```cypher
MATCH (a:Address)
WHERE a.address = "1234"
WITH a
MATCH (a)<-[:USES]-(o:Output)
RETURN a.address, sum(o.value)
```

also account for what went through all the entities

```cypher
MATCH (a:Address)
WHERE a.address = "1234"
WITH a
MATCH (a)<-[:USES]-(o:Output)
OPTIONAL MATCH (a)<-[:OWNER_OF]-()-[:OWNER_OF]->(connected_a)<-[:USES]-(connected_o:Output)
WHERE connected_a <> a
RETURN a.address, sum(o.value)+sum(connected_o.value)
```


<a name="own-bitcoin-node"></a>
## Using your own Bitcoin node 

it is recommended to run your own bitcoin, however you should look at whether you have the right hardware for that.

### Bitcoin Core setup and configuration

First, install the current version of Bitcoin Core: 

`dnf install \'bitcoin-core\*\'`
`apt-get install bitcoind bitcoin-qt bitcoin-tx`

Make sure the version is `bitcoind >= 0.22`

You can also install from [source](https://github.com/bitcoin/bitcoin) or from a 
[pre-compiled executable](https://bitcoin.org/en/download) if you are so inclined.

Once installed, you'll have access to three programs: `bitcoind` (= full peer), `bitcoin-qt` (= peer with GUI),
and `bitcoin-cli` (RPC command line interface). 

Depending on how you installed bitcoind, you will find a sample configuration 
file somewhere, perhaps as /usr/share/doc/bitcoin-core-server/bitcoin.conf.example. 
Place a copy of it in $HOME/.bitcoin/bitcoin.conf as the user who will run 
bitcoind and edit it at least as follows:

    # server=1 tells Bitcoin-QT to accept JSON-RPC commands.
    server=1

    # You must set rpcauth to secure the JSON-RPC api. rpcauth and rpcpassword 
    # are obsolete. Use rcpauth.py from your bitcoind installation or from 
    # [github]https://github.com/bitcoin/bitcoin/blob/master/share/rpcauth/rpcauth.py 
    # to create the password hash. 
    #rpcuser=your_rpcuser 
    #rpcpassword=your_rpcpass
    rcpauth=your_rpcuser:password_hash

    # How many seconds bitcoin will wait for a complete RPC HTTP request.
    # after the HTTP connection is established.
    rpctimeout=300

    # Listen for RPC connections on this TCP port:
    rpcport=8332
    
    # Index non-wallet transactions (required for fast txn and block lookups)
    txindex=1

    # Enable unauthenticated REST API
    rest=1

    # Do NOT enable pruning
    prune=0

    # Configure this if you don't have 800-900 GB free space in the 
    # bitcoind user's home directory
    datadir=/path/to/lots/of/free/space

If you already had a working bitcoind with some blocks but without indexing, 
run `bitcoind -reindex` before anything else.

Now you should be able to start and run a Bitcoin Core peer as follows:

    `bitcoind -printtoconsole`

Test whether the JSON-RPC interface is working by starting your Bitcoin Core peer (...waiting until it finished
startup...) and using the following cURL request (with adapted username and password):

    `curl --data-binary '{"jsonrpc": "1.0", "id":"curltext", "method": "getblockchaininfo", "params": [] }' -H 'content-type: text/plain;' http://your_rpcuser:your_rpcpass@localhost:8332/`

Test non-wallet transaction data access by taking an arbitrary transaction id and issuing the following request:

    `curl --data-binary '{"jsonrpc": "1.0", "id":"curltext", "method": "getrawtransaction", "params": ["110ed92f558a1e3a94976ddea5c32f030670b5c58c3cc4d857ac14d7a1547a90", 1] }' -H 'content-type: text/plain;' http://your_rpcuser:your_rpcpass@localhost:8332/`

Test the REST interface using some sample block hash

    http://localhost:8332/rest/block/000000000000000e7ad69c72afc00dc4e05fc15ae3061c47d3591d07c09f2928.json

When you have reached this point, your Bitcoin Core setup is working. You can let it run on, 
or relaunch it as a background daemon with `bitcoind -daemon`.
