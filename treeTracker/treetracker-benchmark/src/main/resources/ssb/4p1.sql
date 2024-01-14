-- implement the select predicates as views on top of the tables for SSB
-- instruction: psql -p5432 -d postgres -f treeTracker/treetracker-benchmark/src/main/resources/ssb/4p1.sql

BEGIN TRANSACTION;

DROP VIEW IF EXISTS ssb.q4p1_customer CASCADE;
DROP VIEW IF EXISTS ssb.q4p1_supplier CASCADE;
DROP VIEW IF EXISTS ssb.q4p1_part CASCADE;

CREATE VIEW ssb.q4p1_customer as
SELECT custkey
FROM ssb.customer
WHERE c_region = 'AMERICA';

CREATE VIEW ssb.q4p1_supplier as
SELECT suppkey
FROM ssb.supplier
WHERE s_region = 'AMERICA';

CREATE VIEW ssb.q4p1_part as
SELECT PARTKEY
FROM ssb.part
WHERE (p_mfgr = 'MFGR#1' or p_mfgr = 'MFGR#2');

END;
