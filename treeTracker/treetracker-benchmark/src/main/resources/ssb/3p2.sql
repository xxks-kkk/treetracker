-- implement the select predicates as views on top of the tables for SSB
-- instruction: psql -p5432 -d postgres -f treeTracker/treetracker-benchmark/src/main/resources/ssb/3p2.sql

BEGIN TRANSACTION;

DROP VIEW IF EXISTS ssb.q3p2_customer CASCADE;
DROP VIEW IF EXISTS ssb.q3p2_supplier CASCADE;
DROP VIEW IF EXISTS ssb.q3p2_date CASCADE;

CREATE VIEW ssb.q3p2_customer as
SELECT custkey
FROM ssb.customer
WHERE c_nation = 'UNITED STATES';

CREATE VIEW ssb.q3p2_supplier as
SELECT suppkey
FROM ssb.supplier
WHERE s_nation = 'UNITED STATES';

CREATE VIEW ssb.q3p2_date as
SELECT DATEKEY
FROM ssb.date
WHERE d_year >= 1992 and d_year <= 1997;

END;
