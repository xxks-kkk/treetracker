#!/bin/bash -xv

PROJECT_ROOT=/home/zeyuanhu/projects/treetracker2
DB_LOCATION=${PROJECT_ROOT}/treeTracker/ssb-s1.duckdb

duckdb ${DB_LOCATION} < ${PROJECT_ROOT}/treeTracker/treetracker-benchmark/src/main/resources/ssb/1p1.sql &&
duckdb ${DB_LOCATION} < ${PROJECT_ROOT}/treeTracker/treetracker-benchmark/src/main/resources/ssb/1p2.sql &&
duckdb ${DB_LOCATION} < ${PROJECT_ROOT}/treeTracker/treetracker-benchmark/src/main/resources/ssb/1p3.sql &&
duckdb ${DB_LOCATION} < ${PROJECT_ROOT}/treeTracker/treetracker-benchmark/src/main/resources/ssb/2p1.sql &&
duckdb ${DB_LOCATION} < ${PROJECT_ROOT}/treeTracker/treetracker-benchmark/src/main/resources/ssb/2p2.sql &&
duckdb ${DB_LOCATION} < ${PROJECT_ROOT}/treeTracker/treetracker-benchmark/src/main/resources/ssb/2p3.sql &&
duckdb ${DB_LOCATION} < ${PROJECT_ROOT}/treeTracker/treetracker-benchmark/src/main/resources/ssb/3p1.sql &&
duckdb ${DB_LOCATION} < ${PROJECT_ROOT}/treeTracker/treetracker-benchmark/src/main/resources/ssb/3p2.sql &&
duckdb ${DB_LOCATION} < ${PROJECT_ROOT}/treeTracker/treetracker-benchmark/src/main/resources/ssb/3p3.sql &&
duckdb ${DB_LOCATION} < ${PROJECT_ROOT}/treeTracker/treetracker-benchmark/src/main/resources/ssb/3p4.sql &&
duckdb ${DB_LOCATION} < ${PROJECT_ROOT}/treeTracker/treetracker-benchmark/src/main/resources/ssb/4p1.sql &&
duckdb ${DB_LOCATION} < ${PROJECT_ROOT}/treeTracker/treetracker-benchmark/src/main/resources/ssb/4p2.sql &&
duckdb ${DB_LOCATION} < ${PROJECT_ROOT}/treeTracker/treetracker-benchmark/src/main/resources/ssb/4p3.sql