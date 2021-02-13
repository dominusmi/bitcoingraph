# P2PK Address Fix

## Problem

Bitcoin Core developers have removed the `address` object from P2PK transactions on `bitcoind`. 

https://github.com/bitcoin/bitcoin/pull/16725

This change impacts REST and RPC methods used by `bitcoingraph` such as `getrawtransaction`.

For example, transaction `8c14f0db3df150123e6f3dbbf30f8b955a8249b62ac1d1ff16284aefa3d06d87` 
have different outputs in the old vs new versions of `bitcoind`
    
**Old Output**
    
    {
      "blockhash": "000000000002d01c1fccc21636b607dfd930d31d01c3a62104612a1719011250",
      "blocktime": 1293623731,
      "confirmations": 233618,
      "hex": "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff07044c86041b013effffffff0100f2052a01000000434104d190840cfdae05af3d2febca52b0466b6efb02b44036a5d0d70659a53f7b84b736c5a05ed81e90af70985d59ffb3d1b91364f70b4d2b3b7553e177b1ceaff322ac00000000",
      "locktime": 0,
      "time": 1293623731,
      "txid": "110ed92f558a1e3a94976ddea5c32f030670b5c58c3cc4d857ac14d7a1547a90",
      "version": 1,
      "vin": [
        {
          "coinbase": "044c86041b013e",
          "sequence": 4294967295
        }
      ],
      "vout": [
        {
          "value": 50,
          "n": 0,
          "scriptPubKey": {
            "asm": "04d190840cfdae05af3d2febca52b0466b6efb02b44036a5d0d70659a53f7b84b736c5a05ed81e90af70985d59ffb3d1b91364f70b4d2b3b7553e177b1ceaff322 OP_CHECKSIG",
            "hex": "4104d190840cfdae05af3d2febca52b0466b6efb02b44036a5d0d70659a53f7b84b736c5a05ed81e90af70985d59ffb3d1b91364f70b4d2b3b7553e177b1ceaff322ac",
            "reqSigs": 1,
            "type": "pubkey",
            "addresses": [
              "1XPLDXBheQyN2JCujEYTdHHxz66i3QJJA"
            ]
          }
        }
      ]
    }

**New Output**
    
    {
        "blockhash": "000000000002d01c1fccc21636b607dfd930d31d01c3a62104612a1719011250",
        "blocktime": 1293623731,
        "confirmations": 276562,
        "hash": "110ed92f558a1e3a94976ddea5c32f030670b5c58c3cc4d857ac14d7a1547a90",
        "hex": "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff07044c86041b013effffffff0100f2052a01000000434104d190840cfdae05af3d2febca52b0466b6efb02b44036a5d0d70659a53f7b84b736c5a05ed81e90af70985d59ffb3d1b91364f70b4d2b3b7553e177b1ceaff322ac00000000",
        "locktime": 0,
        "size": 134,
        "time": 1293623731,
        "txid": "110ed92f558a1e3a94976ddea5c32f030670b5c58c3cc4d857ac14d7a1547a90",
        "version": 1,
        "vin": [
            {
                "coinbase": "044c86041b013e",
                "sequence": 4294967295
            }
        ],
        "vout": [
            {
                "value": 50,
                "n": 0,
                "scriptPubKey": {
                    "asm": "04d190840cfdae05af3d2febca52b0466b6efb02b44036a5d0d70659a53f7b84b736c5a05ed81e90af70985d59ffb3d1b91364f70b4d2b3b7553e177b1ceaff322 OP_CHECKSIG",
                    "hex": "4104d190840cfdae05af3d2febca52b0466b6efb02b44036a5d0d70659a53f7b84b736c5a05ed81e90af70985d59ffb3d1b91364f70b4d2b3b7553e177b1ceaff322ac",
                    "type": "pubkey"
                }
            }
        ],
        "vsize": 134,
        "weight": 536
    }

