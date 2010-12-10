#!/usr/bin/env bash

mkdir tpch
cd tpch

curl -LO http://www.tpc.org/tpch/spec/tpch_2_13_0.zip
unzip tpch_*
rm tpch_*.zip
