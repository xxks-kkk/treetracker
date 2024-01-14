-- implement the select predicates as views on top of the tables for SSB
-- instruction: psql -p5432 -d postgres -f treeTracker/treetracker-benchmark/src/main/resources/ssb/4p3.sql

BEGIN TRANSACTION;

DROP VIEW IF EXISTS ssb.q4p3_customer CASCADE;
DROP VIEW IF EXISTS ssb.q4p3_supplier CASCADE;
DROP VIEW IF EXISTS ssb.q4p3_part CASCADE;
DROP VIEW IF EXISTS ssb.q4p3_date CASCADE;

CREATE VIEW ssb.q4p3_customer as
SELECT custkey
FROM ssb.customer
WHERE c_nation = 'UNITED STATES';

CREATE VIEW ssb.q4p3_supplier as
SELECT suppkey
FROM ssb.supplier
WHERE s_nation = 'UNITED STATES';

CREATE VIEW ssb.q4p3_part as
SELECT PARTKEY
FROM ssb.part
WHERE p_category = 'MFGR#14';

CREATE VIEW ssb.q4p3_date as
SELECT DATEKEY
FROM ssb.date
WHERE (d_year = 1997 or d_year = 1998);

END;
