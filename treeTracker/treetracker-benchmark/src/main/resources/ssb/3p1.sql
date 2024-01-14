-- implement the select predicates as views on top of the tables for SSB
-- instruction: psql -p5432 -d postgres -f treeTracker/treetracker-benchmark/src/main/resources/ssb/3p1.sql

BEGIN TRANSACTION;

DROP VIEW IF EXISTS ssb.q3p1_customer CASCADE;
DROP VIEW IF EXISTS ssb.q3p1_supplier CASCADE;
DROP VIEW IF EXISTS ssb.q3p1_date CASCADE;

CREATE VIEW ssb.q3p1_customer as
SELECT custkey
FROM ssb.customer
WHERE c_region = 'ASIA';

CREATE VIEW ssb.q3p1_supplier as
SELECT suppkey
FROM ssb.supplier
WHERE s_region = 'ASIA';

CREATE VIEW ssb.q3p1_date as
SELECT DATEKEY
FROM ssb.date
WHERE d_year >= 1992 and d_year <= 1997;

END;
