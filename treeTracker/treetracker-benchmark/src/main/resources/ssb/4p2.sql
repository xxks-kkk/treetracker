-- implement the select predicates as views on top of the tables for SSB
-- instruction: psql -p5432 -d postgres -f treeTracker/treetracker-benchmark/src/main/resources/ssb/4p2.sql

BEGIN TRANSACTION;

DROP VIEW IF EXISTS ssb.q4p2_customer CASCADE;
DROP VIEW IF EXISTS ssb.q4p2_supplier CASCADE;
DROP VIEW IF EXISTS ssb.q4p2_part CASCADE;
DROP VIEW IF EXISTS ssb.q4p2_date CASCADE;

CREATE VIEW ssb.q4p2_customer as
SELECT custkey
FROM ssb.customer
WHERE c_region = 'AMERICA';

CREATE VIEW ssb.q4p2_supplier as
SELECT suppkey
FROM ssb.supplier
WHERE s_region = 'AMERICA';

CREATE VIEW ssb.q4p2_part as
SELECT PARTKEY
FROM ssb.part
WHERE (p_mfgr = 'MFGR#1'
    or p_mfgr = 'MFGR#2');

CREATE VIEW ssb.q4p2_date as
SELECT DATEKEY
FROM ssb.date
WHERE (d_year = 1997 or d_year = 1998);

END;
