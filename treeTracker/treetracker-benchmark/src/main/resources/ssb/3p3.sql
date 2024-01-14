-- implement the select predicates as views on top of the tables for SSB
-- instruction: psql -p5432 -d postgres -f treeTracker/treetracker-benchmark/src/main/resources/ssb/3p3.sql

BEGIN TRANSACTION;

DROP VIEW IF EXISTS ssb.q3p3_customer CASCADE;
DROP VIEW IF EXISTS ssb.q3p3_supplier CASCADE;
DROP VIEW IF EXISTS ssb.q3p3_date CASCADE;

CREATE VIEW ssb.q3p3_customer as
SELECT custkey
FROM ssb.customer
WHERE c_city='UNITED KI1' or c_city='UNITED KI5';

CREATE VIEW ssb.q3p3_supplier as
SELECT suppkey
FROM ssb.supplier
WHERE s_city='UNITED KI1' or s_city='UNITED KI5';

CREATE VIEW ssb.q3p3_date as
SELECT DATEKEY
FROM ssb.date
WHERE d_year >= 1992 and d_year <= 1997;

END;
