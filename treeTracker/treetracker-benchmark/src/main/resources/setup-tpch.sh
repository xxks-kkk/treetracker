#!/bin/bash -xve

# Setup IMDB dataset to be used for benchmarking and testing
PROJECT_ROOT=/home/zeyuanhu/projects/treetracker2

psql -p5432 -d postgres -f ${PROJECT_ROOT}/treeTracker/treetracker-benchmark/src/main/resources/tpch.sql &&
psql -p5432 -d postgres -f ${PROJECT_ROOT}/treeTracker/treetracker-benchmark/src/main/resources/tpch-constraints-analyze.sql &&

psql -p5432 -d postgres -f ${PROJECT_ROOT}/treeTracker/treetracker-benchmark/src/main/resources/tpch-abbreviated.sql &&
psql -p5432 -d postgres -f ${PROJECT_ROOT}/treeTracker/treetracker-benchmark/src/main/resources/tpch-abbreviated-constraints-analyze.sql &&

bash ${PROJECT_ROOT}/treeTracker/treetracker-benchmark/src/main/resources/tpch/setup.sh

DB_LOCATION=/home/zeyuanhu/projects/treetracker2/treeTracker/tpch-s1.duckdb

duckdb ${DB_LOCATION} < ${PROJECT_ROOT}/treeTracker/treetracker-benchmark/src/main/resources/tpch-duckdb.sql &&
duckdb ${DB_LOCATION} < ${PROJECT_ROOT}/treeTracker/treetracker-benchmark/src/main/resources/tpch-abbreviated-duckdb.sql

bash ${PROJECT_ROOT}/treeTracker/treetracker-benchmark/src/main/resources/tpch/duckdb-setup.sh


