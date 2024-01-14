-- implement the select predicates as views on top of the tables for TPC-H
-- instruction: psql -p5432 -d postgres -f treeTracker/treetracker-benchmark/src/main/resources/tpch/8.sql

BEGIN TRANSACTION;

DROP VIEW IF EXISTS tpch.q8w_region CASCADE;
DROP VIEW IF EXISTS tpch.q8w_nation CASCADE;
DROP VIEW IF EXISTS tpch.q8w_nation2 CASCADE;
DROP VIEW IF EXISTS tpch.q8w_supplier CASCADE;
DROP VIEW IF EXISTS tpch.q8w_orders CASCADE;
DROP VIEW IF EXISTS tpch.q8w_part CASCADE;

CREATE VIEW tpch.q8w_region as
SELECT regionkey
FROM tpch.region
WHERE R_NAME = 'AMERICA';

CREATE VIEW tpch.q8w_nation as
SELECT nationkey, regionkey
FROM tpch.nation;

CREATE VIEW tpch.q8w_nation2 as
SELECT nationkey as nationkey2, regionkey as regionkey2
FROM tpch.nation;

CREATE VIEW tpch.q8w_supplier as
SELECT suppkey, nationkey as nationkey2
FROM tpch.supplier;

CREATE VIEW tpch.q8w_orders as
SELECT orderkey, custkey
FROM tpch.orders
WHERE o_orderdate between date '1995-01-01' and date '1996-12-31';

CREATE VIEW tpch.q8w_part as
SELECT partkey
FROM tpch.part
WHERE p_type = 'ECONOMY ANODIZED STEEL';

END;