-- implement the select predicates as views on top of the tables for TPC-H
-- instruction: psql -p5432 -d postgres -f treeTracker/treetracker-benchmark/src/main/resources/tpch/14.sql

BEGIN TRANSACTION;

DROP VIEW IF EXISTS tpch.q14w_lineitem CASCADE;

CREATE VIEW tpch.q14w_lineitem as
SELECT orderkey, partkey, suppkey
FROM tpch.lineitem
WHERE l_shipdate >= date '1995-09-01'
  and l_shipdate < date '1995-09-01' + interval '1' month;

END;