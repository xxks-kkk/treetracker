#!/bin/bash -xv

# Setup IMDB dataset to be used for benchmarking and testing
PROJECT_ROOT=/home/zeyuanhu/projects/treetracker2

psql -p5432 -d postgres -f ${PROJECT_ROOT}/treeTracker/treetracker-benchmark/src/main/resources/imdb.sql &&
psql -p5432 -d postgres -f ${PROJECT_ROOT}/treeTracker/treetracker-benchmark/src/main/resources/imdb-update-null.sql &&
psql -p5432 -d postgres -f ${PROJECT_ROOT}/treeTracker/treetracker-benchmark/src/main/resources/imdb-constraints-analyze.sql &&

psql -p5432 -d postgres -f ${PROJECT_ROOT}/treeTracker/treetracker-benchmark/src/main/resources/imdb-abbreviated-int.sql &&
psql -p5432 -d postgres -f ${PROJECT_ROOT}/treeTracker/treetracker-benchmark/src/main/resources/imdb-abbreviated-int-update-null.sql &&
psql -p5432 -d postgres -f ${PROJECT_ROOT}/treeTracker/treetracker-benchmark/src/main/resources/imdb-abbreviated-int-constraints-analyze.sql &&

bash ${PROJECT_ROOT}/treeTracker/treetracker-benchmark/src/main/resources/job/setup.sh

duckdb /home/zeyuanhu/projects/treetracker2/main.duckdb < /home/zeyuanhu/projects/treetracker2/treeTracker/treetracker-benchmark/src/main/resources/imdb-duckdb.sql &&
duckdb /home/zeyuanhu/projects/treetracker2/main.duckdb < /home/zeyuanhu/projects/treetracker2/treeTracker/treetracker-benchmark/src/main/resources/imdb-duckdb-update-null.sql &&
duckdb /home/zeyuanhu/projects/treetracker2/main.duckdb < /home/zeyuanhu/projects/treetracker2/treeTracker/treetracker-benchmark/src/main/resources/imdb-abbreviated-int-duckdb.sql &&
duckdb /home/zeyuanhu/projects/treetracker2/main.duckdb < /home/zeyuanhu/projects/treetracker2/treeTracker/treetracker-benchmark/src/main/resources/imdb-abbreviated-int-duckdb-update-null.sql &&

bash ${PROJECT_ROOT}/treeTracker/treetracker-benchmark/src/main/resources/job/duckdb_setup.sh


