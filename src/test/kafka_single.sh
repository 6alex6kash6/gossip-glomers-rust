#!/bin/bash

cd $(pwd)
cargo build --bin kafka_single
./maelstrom test -w kafka --bin ./target/debug/kafka_single --node-count 1 --concurrency 2n --time-limit 20 --rate 1000