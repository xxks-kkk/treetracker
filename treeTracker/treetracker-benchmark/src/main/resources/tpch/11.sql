-- implement the select predicates as views on top of the tables for TPC-H
-- instruction: psql -p5432 -d postgres -f treeTracker/treetracker-benchmark/src/main/resources/tpch/11.sql

BEGIN TRANSACTION;

DROP VIEW IF EXISTS tpch.q11w_nation CASCADE;

CREATE VIEW tpch.q11w_nation as
SELECT nationkey, regionkey
FROM tpch.nation
WHERE n_name = 'GERMANY';

END;