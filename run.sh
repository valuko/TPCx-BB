#!/bin/bash

if [$1 -eq "spark"] then
    ./bin/bigBench runBenchmark -d bigbench -f 300 -m 80 -s 6 -b -e spark_sql
else
    ./bin/bigBench runBenchmark -d bigbench -f 300 -m 80 -s 6 -b -e hive
fi
