# Table of contents
- [Neo4j pointers and examples](#neo4j-pointers)
- [Using your own bitcoin node](#own-bitcoin-node)
- [Compute entities extended explanation](#entities-process)

<a name="hardware-requirements"></a>
## Hardware requirements

**Network**: Before anything else, you will have to download the entire blockchain up to the last block that interests
you. In July 2023 the full blockchain is a download of about 525 GiB. You will need this on the same machine (or at
least on the same LAN) as bitcoingraph and your neo4j database.

**Storage**: Exporting the blockchain and importing it into neo4j does some very intensive I/O. You are advised to use
the fastest possible NVMe for writing and, whenever possible, separate reading from writing onto different storage
devices. In a first step you will run bitcoind and dump the entire blockchain into various CSV files. You can easily put
bitcoind and its BTC blocks on a traditional metal HDD (the full blockchain with indices and other auxiliary files is
about 575 GiB in July 2023), but you should be writing the CSV files on a different device, preferably an NVMe. A neo4j
database of the full blockchain (again, July 2023) will need more than 2 TB and less than 4 TB of storage space. Big Fat
Warning: do NOT use any copy-on-write or snapshotting filesystem. If you do, you will completely ruin performance. Even
journaling should probably be turned off until the neo4j database is ready and running.

**CPU**: Some (but not all) bitcoingraph processes, most notably the import into neo4j, can use multiple CPUs in
parallel. Then again, the more CPUs that you use, the more likely it is that you will run into your storage's read/write
limits. If you use metal HDDs, one or two CPU cores should be enough. If you use fast NVMes you can experiment with four
to eight cores, also depending on the speed of your CPU.

**RAM**: This is the most expensive part of this operation. For a full blockchain import (July 2023) into neo4j you will
need at least 48 GB of RAM, most likely even more. The original bcgraph-compute-entities
from [source](https://github.com/behas/bitcoingraph/tree/master/scripts) consumed more than 230 GB of RAM for an entities
computation of blocks 0-675000. The current version can do the same job with about 60 GB of RAM. You can
try to tweak this back and for some marginal gains, but you will never achieve any acceptable speed on a low-memory
system. Or any results at all, other than a crash, without altering the code. A wet-finger-in-the-air recommendation is
64 GB of RAM at the very least, preferably more.


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


<a name="entities-process"></a>
## Compute entities extended documentation
Higher up in the README, we explained what an entity is. Computing entities is a simple
process fundamentally, made very hard due to the size of the files. 
We use two files: rel_input.csv and rel_output_address.csv. The first is in
chronological order, and the second is sorted by Output id.

The objective is two-fold: 1. for each output, find the address, and 2. if two addresses
are inputs to the same transaction, create an entity (or merge if one of the two is already
part of an entity).

The `bcgraph-compute-entities` is basically a database specialized for this very specific
file format and objective.