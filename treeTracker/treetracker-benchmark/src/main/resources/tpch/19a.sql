-- implement the select predicates as views on top of the tables for TPC-H
-- instruction: psql -p5432 -d postgres -f treeTracker/treetracker-benchmark/src/main/resources/tpch/19a.sql

BEGIN TRANSACTION;

DROP VIEW IF EXISTS tpch.q19aw_lineitem CASCADE;
DROP VIEW IF EXISTS tpch.q19aw_part CASCADE;

CREATE VIEW tpch.q19aw_lineitem as
SELECT orderkey, partkey, suppkey
FROM tpch.lineitem
WHERE l_quantity >= 1
  and l_quantity <= 1 + 10
  and l_shipmode in ('AIR', 'AIR REG')
  and l_shipinstruct = 'DELIVER IN PERSON';

CREATE VIEW tpch.q19aw_part as
SELECT partkey
FROM tpch.part
WHERE p_brand = 'Brand#12'
  and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
  and p_size between 1 and 5;

END;