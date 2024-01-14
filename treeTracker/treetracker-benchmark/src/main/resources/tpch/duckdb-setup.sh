#!/bin/bash -xv

DB_LOCATION=/home/zeyuanhu/projects/treetracker2/treeTracker/tpch-s1.duckdb
PROJECT_ROOT=/home/zeyuanhu/projects/treetracker2

duckdb ${DB_LOCATION} < ${PROJECT_ROOT}/treeTracker/treetracker-benchmark/src/main/resources/tpch/3.sql &&
duckdb ${DB_LOCATION} < ${PROJECT_ROOT}/treeTracker/treetracker-benchmark/src/main/resources/tpch/7a.sql &&
duckdb ${DB_LOCATION} < ${PROJECT_ROOT}/treeTracker/treetracker-benchmark/src/main/resources/tpch/7b.sql &&
duckdb ${DB_LOCATION} < ${PROJECT_ROOT}/treeTracker/treetracker-benchmark/src/main/resources/tpch/8.sql &&
duckdb ${DB_LOCATION} < ${PROJECT_ROOT}/treeTracker/treetracker-benchmark/src/main/resources/tpch/9.sql &&
duckdb ${DB_LOCATION} < ${PROJECT_ROOT}/treeTracker/treetracker-benchmark/src/main/resources/tpch/10.sql &&
duckdb ${DB_LOCATION} < ${PROJECT_ROOT}/treeTracker/treetracker-benchmark/src/main/resources/tpch/11.sql &&
duckdb ${DB_LOCATION} < ${PROJECT_ROOT}/treeTracker/treetracker-benchmark/src/main/resources/tpch/12.sql &&
duckdb ${DB_LOCATION} < ${PROJECT_ROOT}/treeTracker/treetracker-benchmark/src/main/resources/tpch/14.sql &&
duckdb ${DB_LOCATION} < ${PROJECT_ROOT}/treeTracker/treetracker-benchmark/src/main/resources/tpch/15.sql &&
duckdb ${DB_LOCATION} < ${PROJECT_ROOT}/treeTracker/treetracker-benchmark/src/main/resources/tpch/16.sql &&
duckdb ${DB_LOCATION} < ${PROJECT_ROOT}/treeTracker/treetracker-benchmark/src/main/resources/tpch/18.sql &&
duckdb ${DB_LOCATION} < ${PROJECT_ROOT}/treeTracker/treetracker-benchmark/src/main/resources/tpch/19a.sql &&
duckdb ${DB_LOCATION} < ${PROJECT_ROOT}/treeTracker/treetracker-benchmark/src/main/resources/tpch/19b.sql &&
duckdb ${DB_LOCATION} < ${PROJECT_ROOT}/treeTracker/treetracker-benchmark/src/main/resources/tpch/19c.sql &&
duckdb ${DB_LOCATION} < ${PROJECT_ROOT}/treeTracker/treetracker-benchmark/src/main/resources/tpch/20.sql