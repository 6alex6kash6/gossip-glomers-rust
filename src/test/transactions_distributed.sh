#!/bin/bash

cd $(pwd)
cargo build --bin transactions_distributed
./maelstrom test -w txn-rw-register --bin ./target/debug/transactions_distributed --node-count 2 --concurrency 2n --time-limit 20 --rate 1000 --consistency-models read-committed --availability total --nemesis partition