-- implement the select predicates as views on top of the tables for TPC-H
-- instruction: psql -p5432 -d postgres -f treeTracker/treetracker-benchmark/src/main/resources/tpch/10.sql

BEGIN TRANSACTION;

DROP VIEW IF EXISTS tpch.q10w_orders CASCADE;
DROP VIEW IF EXISTS tpch.q10w_lineitem CASCADE;

CREATE VIEW tpch.q10w_orders as
SELECT orderkey, custkey
FROM tpch.orders
WHERE o_orderdate >= date '1993-10-01'
  and o_orderdate < date '1993-10-01' + interval '3' month;

CREATE VIEW tpch.q10w_lineitem as
SELECT orderkey, partkey, suppkey
FROM tpch.lineitem
WHERE l_returnflag = 'R';

END;