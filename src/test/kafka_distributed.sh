#!/bin/bash

cd $(pwd)
cargo build --bin kafka_distributed
./maelstrom test -w kafka --bin ./target/debug/kafka_distributed --node-count 2 --concurrency 2n --time-limit 20 --rate 1000