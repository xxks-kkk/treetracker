#!/bin/bash -xve

# Setup IMDB dataset to be used for benchmarking and testing
PROJECT_ROOT=/home/zeyuanhu/projects/treetracker2

psql -p5432 -d postgres -f ${PROJECT_ROOT}/treeTracker/treetracker-benchmark/src/main/resources/ssb.sql &&
psql -p5432 -d postgres -f ${PROJECT_ROOT}/treeTracker/treetracker-benchmark/src/main/resources/ssb-constraints-analyze.sql &&

psql -p5432 -d postgres -f ${PROJECT_ROOT}/treeTracker/treetracker-benchmark/src/main/resources/ssb-abbreviated.sql &&
psql -p5432 -d postgres -f ${PROJECT_ROOT}/treeTracker/treetracker-benchmark/src/main/resources/ssb-abbreviated-constraints-analyze.sql &&

bash ${PROJECT_ROOT}/treeTracker/treetracker-benchmark/src/main/resources/ssb/setup.sh

DB_LOCATION=${PROJECT_ROOT}/treeTracker/ssb-s1.duckdb

duckdb ${DB_LOCATION} < ${PROJECT_ROOT}/treeTracker/treetracker-benchmark/src/main/resources/ssb-duckdb.sql &&
duckdb ${DB_LOCATION} < ${PROJECT_ROOT}/treeTracker/treetracker-benchmark/src/main/resources/ssb-abbreviated-duckdb.sql

bash ${PROJECT_ROOT}/treeTracker/treetracker-benchmark/src/main/resources/ssb/duckdb-setup.sh


