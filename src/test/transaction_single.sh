#!/bin/bash

cd $(pwd)
cargo build --bin transactions_single
./maelstrom test -w txn-rw-register --bin ./target/debug/transactions_single --node-count 1 --time-limit 20 --rate 1000 --concurrency 2n --consistency-models read-uncommitted --availability total