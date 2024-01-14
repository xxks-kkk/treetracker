-- implement the select predicates as views on top of the tables for TPC-H
-- instruction: psql -p5432 -d postgres -f treeTracker/treetracker-benchmark/src/main/resources/tpch/19b.sql

BEGIN TRANSACTION;

DROP VIEW IF EXISTS tpch.q19bw_lineitem CASCADE;
DROP VIEW IF EXISTS tpch.q19bw_part CASCADE;

CREATE VIEW tpch.q19bw_lineitem as
SELECT orderkey, partkey, suppkey
FROM tpch.lineitem
WHERE l_quantity >= 10
  and l_quantity <= 10 + 10
  and l_shipmode in ('AIR', 'AIR REG')
  and l_shipinstruct = 'DELIVER IN PERSON';

CREATE VIEW tpch.q19bw_part as
SELECT partkey
FROM tpch.part
WHERE p_brand = 'Brand#23'
  and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
  and p_size between 1 and 10;

END;