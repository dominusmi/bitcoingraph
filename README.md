Bitcoingraph - A Python library for exploring the Bitcoin transaction graph.

## Difference with the "original" repository

The original repository was created in 2015 as a thesis for a university master degree. Since 2015, the 
blockchain bitcoin has changed a lot.

In 2021, author (s7p)[https://github.com/s7p] made a fork to add the difficulty, connect blocks together, 
and fix some compatibility issues.

This fork contains large refactorings. On the compatibility side, the addresses were not sent back in the 
same format as they used to by bitcoind service (see https://github.com/btcsuite/btcd/issues/1874).
However, a much larger problem was the entity computation. The previous script was made in 2015, with the 
entire blockchain weighting a few GBs. Nowadays, it weights close to 1TB. The script was not adapted to 
this modern issue, and would require hundreds of GBs (we stopped testing after 200GB, the exact number is unknown).

This required a complete overhaul of the entity computation. The script now contains arguments to limit the amount 
of memory used, meaning you can probably run it on 16GB (I wouldn't personally, but it will work). There's a section
below regarding requirements and giving more details.


## Prerequesites

### OS Note
The code can only be run on UNIX compatible systems, as it makes use of `sort` and `uniq` terminal commands. 
The newer versions were only tested on linux, but the modifications made should not affect MAC OSX.
It was not tested on windows, it will not work on "native" windows, but could potentially work if run through 
some linux virtualisation (e.g. WSL) or some UNIX terminal system.

### Bitcoin Core setup and configuration

First, install the current version of Bitcoin Core (v.11.1), either from [source](https://github.com/bitcoin/bitcoin) or
from a [pre-compiled executable](https://bitcoin.org/en/download).

Once installed, you'll have access to three programs: `bitcoind` (= full peer), `bitcoin-qt` (= peer with GUI),
and `bitcoin-cli` (RPC command line interface). The following instructions have been tested with `bitcoind` and assume
you can start and run a Bitcoin Core peer as follows:

    bitcoind -printtoconsole

Second, you must make sure that your bitcoin client accepts JSON-RPC connections by modifying
the [Bitcoin Core configuration file][bc_conf] as follows:

    # server=1 tells Bitcoin-QT to accept JSON-RPC commands.
    server=1

    # You must set rpcuser and rpcpassword to secure the JSON-RPC api
    rpcuser=your_rpcuser
    rpcpassword=your_rpcpass

    # How many seconds bitcoin will wait for a complete RPC HTTP request.
    # after the HTTP connection is established.
    rpctimeout=30

    # Listen for RPC connections on this TCP port:
    rpcport=8332
    
    # Index non-wallet transactions (required for fast txn and block lookups)
    txindex=1

    # Enable unauthenticated REST API
    rest=

Test whether the JSON-RPC interface is working by starting your Bitcoin Core peer (...waiting until it finished
startup...) and using the following cURL request (with adapted username and password):

    curl --data-binary '{"jsonrpc": "1.0", "id":"curltext", "method": "getblockchaininfo", "params": [] }' -H 'content-type: text/plain;' http://your_rpcuser:your_rpcpass@localhost:8332/

Third, since Bitcoingraph needs to access non-wallet blockchain transactions by their ids, you need to enable the
transaction index in the Bitcoin Core database. This can be achieved by adding the following property to
your `bitcoin.conf`

    txindex=1

... and restarting your Bitcoin core peer as follows (rebuilding the index can take a while):

    bitcoind -reindex

Test non-wallet transaction data access by taking an arbitrary transaction id and issuing the following request using
cURL:

    curl --data-binary '{"jsonrpc": "1.0", "id":"curltext", "method": "getrawtransaction", "params": ["110ed92f558a1e3a94976ddea5c32f030670b5c58c3cc4d857ac14d7a1547a90", 1] }' -H 'content-type: text/plain;' http://your_rpcuser:your_rpcpass@localhost:8332/

Finally, bitcoingraph also makes use of Bitcoin Core's HTTP REST interface, which is enabled using the following
parameter:

    bitcoind -rest

Test it using some sample block hash

    http://localhost:8332/rest/block/000000000000000e7ad69c72afc00dc4e05fc15ae3061c47d3591d07c09f2928.json

When you reached this point, your Bitcoin Core setup is working. Terminate all running bitcoind instances and launch a
new background daemon with enabled REST interface

    bitcoind -daemon -rest

### Bitcoingraph library setup

Bitcoingraph is being developed in Python 3.9 Make sure it is running on your machine:

    python --version

...test and install the Bitcoingraph library:

    cd bitcoingraph
    pip install -r requirements.txt
    py.test
    python setup.py install

### Hardware

The resources needed for creating the graph database are roughly proportional to the size of the database, up to some
limit. You could do testing and development with a tiny subset of all bitcoin transactions, e.g. the first 10000 blocks,
even on a Raspberry Pi. If you plan to import the entire blockchain, you will need much more serious hardware.

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
from [source](https://github.com/behas/bitcoingraph/tree/master/scripts) consumed 230 GB of RAM for an entities
computation of blocks 0-675000. The current version can do the same job with about 60 GB of RAM. You can
try to tweak this back and fro for some marginal gains, but you will never achieve any acceptable speed on a low-memory
system. Or any results at all, other than a crash, without altering the code. A wet-finger-in-the-air recommendation is
64 GB of RAM at the very least, preferably more.

### Mac OSX specifics

Running bitcoingraph on a Mac requires coreutils to be installed

    homebrew install coreutils

## What it creates

## Visualisation

![Visualisation](docs/graph.png)

### Explanation:

- `Block`: a bitcoin block, with property `height`. Appends to the previous block to create the chain.


- `Transaction`: a bitcoin transaction, with property `txid`


- `Output`: Output created from a transaction, which is then used as input for a later transaction. Contains the
  property `txid_n`, where `n` is the index of the output, and float `value` as the BTC value. A transaction `123` with
  2 outputs will create two nodes `123_1` and `123_2` both attached as in the outward direction

  ```(:Transaction)-[:OUTPUT]->(:Output)```

  When these outputs are used as input in a later transaction, a new link will be added:

  ```(:Transaction)-[:OUTPUT]->(:Output)-[:INPUT]->(newTransaction:Transaction)```


- `Address`: a bitcoin address, with property `address`. Old addresses using public keys are prefixed by `pk_`. The
  latter also generate their P2PKH and P2WPKH addresses, which are connected through the
  `(publicKey:Address)-[:GENERATES]->(p2pkh:Address)` relationship


- `Entity`: an entity is an extra node which is not part of the blockchain. It is computed in post-processing and is
  used to connect addresses that were used as input in the same transaction, basically making the assumption that it
  implies they come from the same "Entity". Entities are merged together, meaning for example:
    - Transaction `t1` receives inputs from addresses `a1`,`a2`,`a3`
        - an entity is created connecting these addresses, `e1`
    - Transaction `t2` receives inputs from addresses `a2`,`a4`
        - since `a2` is already part of an entity, then `a4` is added to that same entity `e1`

## Boostrapping the underlying graph database (Neo4J)

bitcoingraph stores Bitcoin transactions as directed labelled graph in a Neo4J graph database instance. This database
can be bootstrapped by loading an initial blockchain dump, performing entity computation over the entire dump as
described by [Ron and Shamir](https://eprint.iacr.org/2012/584.pdf), and ingesting it into a running Neo4J instance.

#### Important note
When we took over this project, it had last been used on data from 2016. We had to entirely 
re-write parts of the codebase due to the fact that the total bitcoin blockchain size at the time was
a couple of GBs, whereas nowadays it's closer to 1TB. Many of the processes were not adapted for the size.

I would strongly suggest anyone wanting to do this on the real blockchain, to first do the whole process
at small scale. Using only the first 200k blocks, the entire process can be done on any average modern laptop in less 
than 2 hours (most of which will be waiting for computations). 
This way, one can get comfortable with the process and try the database at small scale. At real scale, the process takes
a couple of days total in various computation, hence why it's better to do a trial run first.

### Step 1: Create transaction dump from blockchain

Bitcoingraph provides the `bcgraph-export` tool for exporting transactions in a given block range from the blockchain.
The following command exports all transactions contained in block range 0 to 1000 using Neo4Js header format and
separate CSV header files:

    bcgraph-export 0 1000 -u your_rpcuser -p your_rpcpass

The following CSV files are created (with separate header files):

* **addresses.csv**: sorted list of Bitcoin addressed
* **blocks.csv**: list of blocks (hash, height, timestamp)
* **transactions.csv**: list of transactions (hash, coinbase/non-coinbase)
* **outputs.csv**: list of transaction outputs (output key, id, value, script type)
* **rel_block_tx.csv**: relationship between blocks and transactions (block_hash, tx_hash)
* **rel_input.csv**: relationship between transactions and transaction outputs (tx_hash, output key)
* **rel_output_address.csv**: relationship between outputs and addresses (output key, address)
* **rel_tx_output.csv**: relationship between transactions and transaction outputs (tx_hash, output key)

### Step 2: Compute entities over transaction dump


#### 2.1: Compute the entities
The following command computes entities for a given blockchain data dump:

    bcgraph-compute-entities -i blocks_0_1000 


This script is extremely computationally intensive, both in memory and in processing.
There are various parameters that can be tuned to optimize performance:

`--read-size`: Number of bytes to read at once from the file

`--chunk-size`: Size of a batch to process at once (in bytes)

`--cached-batches`: Number of last processed batches to keep in memory (uses a circular buffer)

`--max-queue-size`: Number of outputs to process together. This is the most important variable
both in terms of performance and memory usage. The higher the better.

On our machine with 110G and AMD Ryzen 5 5600G, we used the following parameters:
```commandline
--cached-batches 5_000 --chunk-size 50_000 --read-size 100_000_000 --max-queue-size 5_000_000_000
```
and reached max usage of 65G of RAM, and took ~15 hours to complete.


#### 2.2: Merge the entities together

Once the entities are computed, we also need to run the following
```bash
cd merge-entities && cargo run --release /path/to/rel_entity_address.csv /path/to/rel_entity_address_merged.csv
```
The first generates computes entities, but due to the size of the file, it has to be 
done in pieces. Therefore, if the entire entities happen to be over to pieces of the 
file, it wrongly creates two entities. That's the reason for merge-entities script,
which is written in `rust` for performance purposes and merged all entities that 
were separated back together. The second argument is the output file, in theory it can 
be the same as the input, but what we used (and is used throughout this README) is simply
adding the _merged suffix.


Two additional files are created:

* entities.csv: list of entity identifiers (entity_id)
* rel_address_entity_merged.csv: assignment of addresses to entities (entity_id, address)

#### Note: what is this about?
Higher up in the README, we explained what an entity is. Computing entities is a simple
process fundamentally, made very hard due to the size of the files. 
We use two files: rel_input.csv and rel_output_address.csv. The first is in
chronological order, and the second is sorted by Output id.

The objective is two-fold: 1. for each output, find the address, and 2. if two addresses
are inputs to the same transaction, create an entity (or merge if one of the two is already
part of an entity).

The `bcgraph-compute-entities` is basically a database specialized for this very specific
file format and objective.

### Step 3: Compute P2PKH and P2WPKH addresses
The raw data doesn't include the connection between addresses in the format public key, and the 
P2PKH and P2WPKH addresses that are "generated" by the latter. This script computes all the generated 
addresses, and creates a file `rel_address_address.csv` and `rel_address_address_header.csv`.

```
bcgraph-pk-to-addresses -i blocks_0_1000
```

### Step 4: Ingest pre-computed dump into Neo4J

Download and install [Neo4J][neo4j] community edition (>= 5.0.0):

    tar xvfz neo4j-community-2.3.0-unix.tar.gz

Test Neo4J installation:

    sudo neo4j start
    http://localhost:7474/

Install and make sure is not running and pre-existing databases are removed:

    sudo neo4j stop
    sudo rm -rf /var/lib/neo4j/data/*

Switch back into the dump directory and create a new database using Neo4J's CSV importer tool:

```bash
neo4j-admin database import full --overwrite-destination 
  --nodes=:Block=blocks_header.csv,blocks.csv 
  --nodes=:Transaction=transactions_header.csv,transactions.csv 
  --nodes=:Output=outputs_header.csv,outputs.csv
  --nodes=:Address=addresses_header.csv,addresses.csv 
  --relationships=CONTAINS=rel_block_tx_header.csv,rel_block_tx.csv 
  --relationships=APPENDS=rel_block_block_header.csv,rel_block_block.csv 
  --relationships=OUTPUT=rel_tx_output_header.csv,rel_tx_output.csv 
  --relationships=INPUT=rel_input_header.csv,rel_input.csv 
  --relationships=USES=rel_output_address_header.csv,rel_output_address_merged.csv 
  --nodes=:Entity=entity_header.csv,entity.csv 
  --relationships=OWNER_OF=rel_entity_address_header.csv,rel_entity_address.csv  
  --relationships=GENERATES=rel_address_address_header.csv,rel_address_address.csv 
  <database name>
```
Then, start the Neo4J shell...:

    $NEO4J_HOME/bin/neo4j-shell -path $NEO4J_HOME/data

and create the following indexes:

```
    // Allows fast queries using the address (highly recommended)
    CREATE CONSTRAINT FOR (a:Address) REQUIRE a.address IS UNIQUE;
    
    // Allows fast queries using the block height (highly recommended)
    CREATE CONSTRAINT FOR (b:Block) REQUIRE b.height IS UNIQUE;
    
    // Allows fast queries using the output txid_n (Optional)
    CREATE CONSTRAINT FOR (o:Output) REQUIRE o.txid_n IS UNIQUE;

    // Allows fast queries using transaction txid (Optional)
    CREATE CONSTRAINT FOR (t:Transaction) REQUIRE t.txid IS UNIQUE;
    
    // Allows fast queries using entity_id (Optional)
    CREATE CONSTRAINT FOR (e:Entity) REQUIRE e.entity_id IS UNIQUE;
    
    // Allows fast queries using entity name, only if you plan on naming entities. By default
    // no names are present (Optional)
    CREATE INDEX FOR (e:Entity) ON (e.name);
```

Finally start Neo4J

    sudo neo4j start

### Step 4: Enrich transaction graph with identity information

Some bitcoin addresses have associated public identity information. Bitcoingraph provides an example script which
collects information from blockchain.info.

    utils/identity_information.py

The resulting CSV file can be imported into Neo4j with the Cypher statement:

    LOAD CSV WITH HEADERS FROM "file://<PATH>/identities.csv" AS row
    MERGE (a:Address {address: row.address})
    CREATE a-[:HAS]->(i:Identity
      {name: row.tag, link: row.link, source: "https://blockchain.info/"})

### Step 5: Enable synchronization with Bitcoin block chain

Bitcoingraph provides a synchronisation script, which reads blocks from bitcoind and writes them into Neo4j. It is
intended to be called by a cron job which runs daily or more frequent. For performance reasons it is no substitution for
steps 1-3.

    bcgraph-synchronize -s localhost -u RPC_USER -p RPC_PASS -S localhost -U NEO4J_USER -P NEO4J_PASS --rest

## Contributors

* [Bernhard Haslhofer](mailto:bernhard.haslhofer@ait.ac.at)
* [Roman Karl](mailto:roman.karl@ait.ac.at)


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

## Example queries

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

## License

This original library is released under the [MIT license](http://opensource.org/licenses/MIT). All changes on this fork
are released under [GPL 3 license](https://www.gnu.org/licenses/gpl-3.0.html)

[bc_core]: https://github.com/bitcoin/bitcoin "Bitcoin Core"

[bc_conf]: https://en.bitcoin.it/wiki/Running_Bitcoin#Bitcoin.conf_Configuration_File "Bitcoin Core configuration file"

[neo4j]: http://neo4j.com/ "Neo4J"
