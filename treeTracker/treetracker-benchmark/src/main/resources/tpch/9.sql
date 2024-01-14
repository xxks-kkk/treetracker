-- implement the select predicates as views on top of the tables for TPC-H
-- instruction: psql -p5432 -d postgres -f treeTracker/treetracker-benchmark/src/main/resources/tpch/9.sql

BEGIN TRANSACTION;

DROP VIEW IF EXISTS tpch.q9w_part CASCADE;


CREATE VIEW tpch.q9w_part as
SELECT partkey
FROM tpch.part
WHERE p_name like '%green%';

END;