#!/bin/bash

cd $(pwd)
cargo build --bin counter
./maelstrom test -w g-counter --bin ./target/debug/counter --node-count 3 --rate 100 --time-limit 20 --nemesis partition
