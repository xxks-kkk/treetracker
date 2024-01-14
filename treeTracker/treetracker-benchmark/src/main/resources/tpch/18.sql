-- implement the select predicates as views on top of the tables for TPC-H
-- instruction: psql -p5432 -d postgres -f treeTracker/treetracker-benchmark/src/main/resources/tpch/18.sql

BEGIN TRANSACTION;

DROP VIEW IF EXISTS tpch.q18w_orders CASCADE;


CREATE VIEW tpch.q18w_orders as
SELECT orderkey, custkey
FROM tpch.orders
WHERE orderkey in (select orderkey as l_orderkey
                   from tpch.lineitem
                   group by l_orderkey
                   having sum(l_quantity) > 300);

END;