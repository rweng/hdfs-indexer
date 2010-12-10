#!/usr/bin/env bash

mkdir tpch
cd tpch

CC=gcc
DATABASE=DB2
MACHINE=LINUX
WORKLOAD=TPCH

# download
curl -LO http://www.tpc.org/tpch/spec/tpch_2_13_0.zip
unzip tpch_*
rm tpch_*.zip

# compile
sed "s/CC *= *$/CC=$CC/g" makefile.suite | \
sed "s/DATABASE *= *$/DATABASE=$DATABASE/g" | \
sed "s/MACHINE *= *$/MACHINE=$MACHINE/g" | \
sed "s/WORKLOAD *= *$/WORKLOAD=$WORKLOAD/g" > makefile
make && make install
