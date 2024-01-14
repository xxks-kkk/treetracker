-- implement the select predicates as views on top of the tables for TPC-H
-- instruction: psql -p5432 -d postgres -f treeTracker/treetracker-benchmark/src/main/resources/tpch/12.sql

BEGIN TRANSACTION;

DROP VIEW IF EXISTS tpch.q12w_lineitem CASCADE;

CREATE VIEW tpch.q12w_lineitem as
SELECT orderkey, partkey, suppkey
FROM tpch.lineitem
WHERE l_shipmode in ('MAIL', 'SHIP')
AND l_commitdate < l_receiptdate
AND l_shipdate < l_commitdate
AND l_receiptdate >= date '1994-01-01'
AND l_receiptdate < date '1994-01-01' + interval '1' year;

END;