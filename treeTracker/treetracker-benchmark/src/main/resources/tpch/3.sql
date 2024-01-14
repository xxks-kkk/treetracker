-- implement the select predicates as views on top of the tables for TPC-H
-- instruction: psql -p5432 -d postgres -f treeTracker/treetracker-benchmark/src/main/resources/tpch/3.sql

BEGIN TRANSACTION;

DROP VIEW IF EXISTS tpch.q3w_customer CASCADE;
DROP VIEW IF EXISTS tpch.q3w_orders CASCADE;
DROP VIEW IF EXISTS tpch.q3w_lineitem CASCADE;


CREATE VIEW tpch.q3w_customer as
SELECT custkey
FROM tpch.customer
WHERE c_mktsegment = 'BUILDING';

CREATE VIEW tpch.q3w_orders as
SELECT orderkey, custkey
FROM tpch.orders
WHERE o_orderdate < date '1995-03-15';

CREATE VIEW tpch.q3w_lineitem as
SELECT orderkey, partkey, suppkey
FROM tpch.lineitem
WHERE l_shipdate > date '1995-03-15';

END;