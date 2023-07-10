Bitcoingraph - A Python library for exploring the Bitcoin transaction graph.

[![Build Status](https://travis-ci.org/behas/bitcoingraph.svg?branch=master)](https://travis-ci.org/behas/bitcoingraph)

## Prerequesites

### Bitcoin Core setup and configuration

First, install the current version of Bitcoin Core (v.11.1), either from [source](https://github.com/bitcoin/bitcoin) or from a [pre-compiled executable](https://bitcoin.org/en/download).

Once installed, you'll have access to three programs: `bitcoind` (= full peer), `bitcoin-qt` (= peer with GUI), and `bitcoin-cli` (RPC command line interface). The following instructions have been tested with `bitcoind` and assume you can start and run a Bitcoin Core peer as follows:

    bitcoind -printtoconsole

Second, you must make sure that your bitcoin client accepts JSON-RPC connections by modifying the [Bitcoin Core configuration file][bc_conf] as follows:

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

Test whether the JSON-RPC interface is working by starting your Bitcoin Core peer (...waiting until it finished startup...) and using the following cURL request (with adapted username and password):

    curl --data-binary '{"jsonrpc": "1.0", "id":"curltext", "method": "getblockchaininfo", "params": [] }' -H 'content-type: text/plain;' http://your_rpcuser:your_rpcpass@localhost:8332/


Third, since Bitcoingraph needs to access non-wallet blockchain transactions by their ids, you need to enable the transaction index in the Bitcoin Core database. This can be achieved by adding the following property to your `bitcoin.conf`

    txindex=1

... and restarting your Bitcoin core peer as follows (rebuilding the index can take a while):

    bitcoind -reindex


Test non-wallet transaction data access by taking an arbitrary transaction id and issuing the following request using cURL:

    curl --data-binary '{"jsonrpc": "1.0", "id":"curltext", "method": "getrawtransaction", "params": ["110ed92f558a1e3a94976ddea5c32f030670b5c58c3cc4d857ac14d7a1547a90", 1] }' -H 'content-type: text/plain;' http://your_rpcuser:your_rpcpass@localhost:8332/


Finally, bitcoingraph also makes use of Bitcoin Core's HTTP REST interface, which is enabled using the following parameter:

    bitcoind -rest

Test it using some sample block hash

    http://localhost:8332/rest/block/000000000000000e7ad69c72afc00dc4e05fc15ae3061c47d3591d07c09f2928.json


When you reached this point, your Bitcoin Core setup is working. Terminate all running bitcoind instances and launch a new background daemon with enabled REST interface

    bitcoind -daemon -rest


### Bitcoingraph library setup

Bitcoingraph is being developed in Python 3.4. Make sure it is running on your machine:

    python --version


Now clone Bitcoingraph...

    git clone https://github.com/behas/bitcoingraph.git


...test and install the Bitcoingraph library:

    cd bitcoingraph
    pip install -r requirements.txt
    py.test
    python setup.py install


### Mac OSX specifics

Running bitcoingraph on a Mac requires coreutils to be installed

    homebrew install coreutils


## Boostrapping the underlying graph database (Neo4J)

bitcoingraph stores Bitcoin transactions as directed labelled graph in a Neo4J graph database instance. This database can be bootstrapped by loading an initial blockchain dump, performing entity computation over the entire dump as described by [Ron and Shamir](https://eprint.iacr.org/2012/584.pdf), and ingesting it into a running Neo4J instance.

### Step 1: Create transaction dump from blockchain

Bitcoingraph provides the `bcgraph-export` tool for exporting transactions in a given block range from the blockchain. The following command exports all transactions contained in block range 0 to 1000 using Neo4Js header format and separate CSV header files:

    bcgraph-export 0 1000 -u your_rpcuser -p your_rpcpass

The following CSV files are created (with separate header files):

* addresses.csv: sorted list of Bitcoin addressed
* blocks.csv: list of blocks (hash, height, timestamp)
* transactions.csv: list of transactions (hash, coinbase/non-coinbase)
* outputs.csv: list of transaction outputs (output key, id, value, script type)
* rel_block_tx.csv: relationship between blocks and transactions (block_hash, tx_hash)
* rel_input.csv: relationship between transactions and transaction outputs (tx_hash, output key)
* rel_output_address.csv: relationship between outputs and addresses (output key, address)
* rel_tx_output.csv: relationship between transactions and transaction outputs (tx_hash, output key)


### Step 2: Compute entities over transaction dump

The following command computes entities for a given blockchain data dump:

    bcgraph-compute-entities -i blocks_0_1000

Two additional files are created:

* entities.csv: list of entity identifiers (entity_id)
* rel_address_entity.csv: assignment of addresses to entities (address, entity_id)


### Step 3: Ingest pre-computed dump into Neo4J

Download and install [Neo4J][neo4j] community edition (>= 2.3.0):

    tar xvfz neo4j-community-2.3.0-unix.tar.gz

Test Neo4J installation:

    sudo neo4j start
    http://localhost:7474/


Install  and make sure is not running and pre-existing databases are removed:

    sudo neo4j stop
    sudo rm -rf /var/lib/neo4j/data/*


Switch back into the dump directory and create a new database using Neo4J's CSV importer tool:

    sudo neo4j-admin import \
    --nodes=:Block=blocks_header.csv,blocks.csv \
    --nodes=:Transaction=transactions_header.csv,transactions.csv \
    --nodes=:Output=outputs_header.csv,outputs.csv \
    --nodes=:Address=addresses_header.csv,addresses.csv \
    --nodes=:Entity=entities.csv \
    --relationships=CONTAINS=rel_block_tx_header.csv,rel_block_tx.csv \
    --relationships=APPENDS=rel_block_block_header.csv,rel_block_block.csv \
    --relationships=OUTPUT=rel_tx_output_header.csv,rel_tx_output.csv \
    --relationships=INPUT=rel_input_header.csv,rel_input.csv \
    --relationships=USES=rel_output_address_header.csv,rel_output_address.csv \
    --relationships=BELONGS_TO=rel_address_entity.csv


Then, start the Neo4J shell...:

    $NEO4J_HOME/bin/neo4j-shell -path $NEO4J_HOME/data

and create the following uniquness constraints:

    CREATE CONSTRAINT ON (a:Address) ASSERT a.address IS UNIQUE;

    CREATE CONSTRAINT ON (o:Output) ASSERT o.txid_n IS UNIQUE;


Finally start Neo4J

    sudo neo4j start


### Step 4: Enrich transaction graph with identity information

Some bitcoin addresses have associated public identity information. Bitcoingraph provides an example script which collects information from blockchain.info.

    utils/identity_information.py

The resulting CSV file can be imported into Neo4j with the Cypher statement:

    LOAD CSV WITH HEADERS FROM "file://<PATH>/identities.csv" AS row
    MERGE (a:Address {address: row.address})
    CREATE a-[:HAS]->(i:Identity
      {name: row.tag, link: row.link, source: "https://blockchain.info/"})


### Step 5: Install Neo4J entity computation plugin

Clone the git repository and compile from source. This requires Maven and Java JDK to be installed.

    git clone https://github.com/romankarl/entity-plugin.git
    cd entity-plugin
    mvn package

Copy the JAR package into Neo4j's plugin directory.

    service neo4j-service stop
    cp target/entities-plugin-0.0.1-SNAPSHOT.jar $NEO4J_HOME/plugins/
    service neo4j-service start



### Step 6: Enable synchronization with Bitcoin block chain

Bitcoingraph provides a synchronisation script, which reads blocks from bitcoind and writes them into Neo4j. It is intended to be called by a cron job which runs daily or more frequent. For performance reasons it is no substitution for steps 1-3.

    bcgraph-synchronize -s localhost -u RPC_USER -p RPC_PASS -S localhost -U NEO4J_USER -P NEO4J_PASS --rest


## Contributors

* [Bernhard Haslhofer](mailto:bernhard.haslhofer@ait.ac.at)
* [Roman Karl](mailto:roman.karl@ait.ac.at)


# Updates
 
## Visualisation
![Visualisation](docs/graph.png)

### Explanation:

- `Block`: a bitcoin block, with property `height`. Appends to the previous block to create the chain.


- `Transaction`: a bitcoin transaction, with property `txid`


- `Output`: Output created from a transaction, which is then used as input for a later transaction.
Contains the property `txid_n`, where `n` is the index of the output, and float `value` as the BTC value.
A transaction `123` with 2 outputs will create two nodes `123_1` and `123_2` both attached as in the 
outward direction 

   ```(:Transaction)-[:OUTPUT]->(:Output)```

  When these outputs are used as input in a later 
  transaction, a new link will be added:

   ```(:Transaction)-[:OUTPUT]->(:Output)-[:INPUT]->(newTransaction:Transaction)```


- `Address`: a bitcoin address, with property `address`. Old addresses using public keys are prefixed by `pk_`.
The latter also generate their P2PKH and P2WPKH addresses, which are connected through the 
`(publicKey:Address)-[:GENERATES]->(p2pkh:Address)` relationship


- `Entity`: an entity is an extra node which is not part of the blockchain. It is computed in post-processing
and is used to connect addresses that were used as input in the same transaction, basically making the assumption 
that it implies they come from the same "Entity". Entities are merged together, meaning for example:
  - Transaction `t1` receives inputs from addresses `a1`,`a2`,`a3`
    - an entity is created connecting these addresses, `e1`
  - Transaction `t2` receives inputs from addresses `a2`,`a4`
    - since `a2` is already part of an entity, then `a4` is added to that same entity `e1`


## Scripts
1. bcgraph-graphexport: create the CSV files from the bitcoind service
2. bcgraph-compute-entities: computes the entities starting from the CSV files, and saves them in a new file. 
Should be run with `--skip-sort-input` except if the option was already provided in the graphexport.
3. bcgraph-generate-pk: creates the connection `:GENERATES` explained earlier. Runs directly on the database
4. bcgraph-synchronoize: keeps the database in sync with the new transactions. Note: although this can
technically be run to load the entire database, it is a lot slower and highly discouraged. It should only be
used to append to an existing database loaded with the CSVs.

## Neo4j pointers
- Before running a large query, always run the query with `EXPLAIN` first. This shows the plan of the
database calls, and can be very useful to notice a suboptimal query
- Don't be scared of using `WITH` to aggregate results during the query, it can save a lot of time. For example
   ```cypher
   MATCH (a:Address)
   OPTIONAL MATCH (a)-[:BELONGS_TO]->(e:Entity)
   WHERE a.address in ["123","456",..]
   RETURN a,e
   ```
   looks like a good query. However, running it with explain will immediately show that the optional match
   actually matches all of the addresses (completely ignoring the `WHERE` condition). Instead, the correct use would be
   ```cypher
   MATCH (a:Address)
   WHERE a.address in ["123","456",..]
   WITH a
   OPTIONAL MATCH (a)-[:BELONGS_TO]->(e:Entity)
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
OPTIONAL MATCH (a)-[:BELONGS_TO]->()<-[:BELONGS_TO]-(connected_a)<-[:USES]-(connected_o:Output)
WHERE connected_a <> a
RETURN a.address, sum(o.value)+sum(connected_o.value)
```

## License

This original library is released under the [MIT license](http://opensource.org/licenses/MIT).
All changes on this fork are released under [GPL 3 license](https://www.gnu.org/licenses/gpl-3.0.html)

[bc_core]: https://github.com/bitcoin/bitcoin "Bitcoin Core"
[bc_conf]: https://en.bitcoin.it/wiki/Running_Bitcoin#Bitcoin.conf_Configuration_File "Bitcoin Core configuration file"
[neo4j]: http://neo4j.com/ "Neo4J"
